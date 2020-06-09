/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.mv

import scala.collection.mutable
import scala.util.control.Breaks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{AliasIdentifier, IdentifierWithDatabase}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualTo, Expression, Literal, NamedExpression, Not, Or, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{ConstantPropagation, Optimizer}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.mv.MVCompatibleRules._

class MaterializedViewOptimizer(analyzer: Analyzer) extends Optimizer(analyzer.getCatalogManager) {

  override def defaultBatches: Seq[Batch] = Seq(
    Batch("ConstantPropagation", Once, ConstantPropagation),
    Batch("MaterilizedView", Once, MatchMaterilizedView(analyzer)))

}

case class MatchMaterilizedView(analyzer: Analyzer) extends Rule[LogicalPlan] {

  private val MATCH_POLICY_ONCE = "once"

  override def apply(plan: LogicalPlan): LogicalPlan = {

    var parsedSubPlan: Option[LogicalPlan] = None

    // process the query in the following format:
    // 1. select * from * where * group by * => Aggregate(_, _, _)
    // 2. select * from * where * => Project(_, _)
    // 3. select * from * where * group by * order by * => Sort(_, _, Aggregate(_, _, _))
    // 4. select * from * where * order by * => Sort(_, _, Project(_, _))
    // for case 3&4, because the origin plan is sort-project, it will be match the pattern twice:
    // the 1st is as sort-project/agg plan, it will be optimizer as normal
    // the 2nd is as project/agg plan, it won't be optimized again
    plan transform {
      case q@Sort(_, _, a@Aggregate(_, _, _)) if isValidForMaterialized(q) =>
        // for case 3
        val (resultPlan, psp) = rewritePlan(q, Some(a), parsedSubPlan)
        parsedSubPlan = psp
        resultPlan
      case q@Sort(_, _, p@Project(_, _)) if isValidForMaterialized(q) =>
        // for case 4
        val (resultPlan, psp) = rewritePlan(q, Some(p), parsedSubPlan)
        parsedSubPlan = psp
        resultPlan
      case q@Project(_, s@Sort(_, _, _)) if isValidForMaterialized(q) =>
        // a transform of case 4 if outputs are all alias, eg
        // select c1 as c1, c2 as c2 from t1 order by c1
        val (resultPlan, psp) = rewritePlan(q, Some(s), parsedSubPlan)
        parsedSubPlan = psp
        resultPlan
      case q@(Project(_, _) | Aggregate(_, _, _)) if isValidForMaterialized(q) =>
        val (resultPlan, _) = rewritePlan(q, None, parsedSubPlan)
        resultPlan
    }
  }

  // rewrite logical plan if it wasn't parsed before
  // todo: it only works when match failed, if sql can be optimized with materialized view,
  //       there has duplicated optimization process and end with Exception
  private def rewritePlan(
      logicalPlan: LogicalPlan,
      subPlan: Option[LogicalPlan],
      parsedSubPlan: Option[LogicalPlan]): (LogicalPlan, Option[LogicalPlan]) = {
    try {
      // if the parent plan is already parsed, won't parsed it again
      if (parsedSubPlan.isDefined &&
          logicalPlan.simpleString(150).equals(parsedSubPlan.get.simpleString(150))) {
        (logicalPlan, subPlan)
      } else {
        (getMaterializedPlan(logicalPlan), subPlan)
      }
    } catch {
      case ex: Exception =>
        logWarning(s"Optimize with materialized view failed, ${ex.getMessage}")
        (logicalPlan, None)
    }
  }

  def getMaterializedPlan(originPlan: LogicalPlan): LogicalPlan = {
    val (queryPlan, orders, isSort2ndProject) = splitPlanWithSort(originPlan)

    // Initialize all query related auxiliary data structures
    // that will be used throughout query rewriting process
    // Generate query table references
    val (queryTableAliasMap, queryTableSet) = getTableIdentifies(queryPlan)
    if (queryTableSet.isEmpty) {
      throw new TryMaterializedFailedException("can't find table in query")
    }

    // Extract query predicates
    val queryPredicates = extractPredicates(queryPlan)

    // Extract query project list
    val (queryAttRefMap, _) = getAttRefMap(queryPlan, queryTableAliasMap, true)
    val (queryProjectAttRefMap, _, queryGroupByExprs) =
      getSpecificAttRefs(queryPlan, queryTableAliasMap, true)

    // split the predicates into (colEqualPreds, others)
    val queryColEqualPreds = splitColEqualPredicates(queryPredicates)

    checkJoinCondition(queryColEqualPreds, queryTableSet, queryTableAliasMap)

    // Extract query equivalence classes. An equivalence class is a set
    // of columns in the query output that are known to be equal.
    val queryEC = new EquivalenceClasses
    updateEquivalenceClass(queryEC, queryColEqualPreds, queryTableAliasMap)

    var matchedResultSqls = Seq.empty[MatchedResultSql]

    // 3. Iterate through all applicable materializations trying to
    // rewrite the given query
    val loop = new Breaks
    loop.breakable {
      for (materializedView <- getMaterializedViews) {
        val mvPlan = materializedView.plan
        try {
          checkMvPlan(mvPlan)

          // Generate view table references
          val (viewTableAliasMap, viewTableSet) = getTableIdentifies(mvPlan)
          if (viewTableSet.isEmpty) {
            throw new TryMaterializedFailedException("empty table in materialized view")
          }

          // Extract view attribute list
          val (viewAttRefMap, viewAttAliasMap) =
            getAttRefMap(mvPlan, viewTableAliasMap, false)
          val (viewProjectAttRefMap, _, viewGroupByExprs) =
            getSpecificAttRefs(mvPlan, viewTableAliasMap, false)

          // Extract view predicates
          val viewPredicates = extractPredicates(mvPlan)
          val viewColEqualPreds = splitColEqualPredicates(viewPredicates)

          checkJoinCondition(viewColEqualPreds, viewTableSet, viewTableAliasMap)

          val viewEC: EquivalenceClasses = new EquivalenceClasses
          updateEquivalenceClass(viewEC, viewColEqualPreds, viewTableAliasMap)

          var compensateTables: Set[String] = Set.empty
          var compensateJoinPreds: Seq[EqualTo] = Seq.empty

          if (!(queryTableSet == viewTableSet)) {
            // Try to compensate, e.g., for join queries it might be
            // possible to join missing tables with view to compute result.
            // Two supported cases:
            // 1. query tables are subset of view tables,
            // need to check whether PK-FK model configuration is enabled
            // 2. view tables are subset of query tables,
            // add additional tables through joins if possible
            if (queryTableSet.subsetOf(viewTableSet)
              && analyzer.getSqlConf.isMaterializedViewPFModelEnabled) {
              if (!viewEC.contains(queryEC)) {
                throw new TryMaterializedFailedException(
                  "Multiple tables with match type is MATCH_QUERY_PARTIAL," +
                    " but equal columns are not satisfied")
              }
            } else if (viewTableSet.subsetOf(queryTableSet)) {
              if (!queryEC.contains(viewEC)) {
                throw new TryMaterializedFailedException(
                  "Multiple tables with match type is MATCH_VIEW_PARTIAL," +
                    " but equal columns are not satisfied")
              }

              // query has more tables, get compensated information
              val result = compensateViewPartial(
                queryTableSet, viewTableSet, queryColEqualPreds, queryTableAliasMap)
              compensateTables = result._1
              compensateJoinPreds = result._2
              updateEquivalenceClass(viewEC, compensateJoinPreds, viewTableAliasMap)
            } else {
              throw new TryMaterializedFailedException(s"Multiple tables with match type is " +
                s"None, query: $queryTableSet, mv: $viewTableSet")
            }
          } else {
            // check if columns equals expression match,
            // eg, t1.c1 = t2.c1 should be in both query and view
            if (!queryEC.contains(viewEC)) {
              throw new TryMaterializedFailedException(
                "Multiple tables with match type is MATCH_COMPLETE," +
                  " but equal columns are not satisfied")
            }
          }

          // check if order is satisfied with the materialized view
          checkOrdersMatch(orders, viewProjectAttRefMap,
            compensateTables, queryEC, viewEC, queryTableAliasMap)

          // check is project list match, view's outputs + viewMissedIdentifies >= query's outputs
          checkProjectListMatch(queryProjectAttRefMap,
            viewProjectAttRefMap, compensateTables, queryEC, viewEC, queryTableAliasMap)

          // check if group by is satisfied with the materialized view
          // if satisfied, the result group by expression will be:
          // 1. empty (expression in query is the same as in materialized view)
          // 2. non-empty (depend on the result of expression match)
          val (isGroupByMatch, groupByCompensates) =
          isGroupByAttRefsMatch(queryGroupByExprs, viewGroupByExprs,
            compensateTables, viewEC, queryTableAliasMap, viewTableAliasMap)
          if (!isGroupByMatch) {
            throw new TryMaterializedFailedException("Group list is not satisfied")
          }

          // get compensate expression for filter
          val (queryCompensates, unionCompensates) = computeCompensation(
            getFilterCondition(queryPlan), getFilterCondition(mvPlan),
            queryEC, viewEC, queryAttRefMap, viewAttRefMap,
            queryTableAliasMap, viewTableAliasMap)

          // doesn't support union for result
          if (!unionCompensates.isEmpty) {
            throw new TryMaterializedFailedException("Filter list is not satisfied")
          }

          val queryOutputs = MVPlanRewriteRules.filterOutputList(queryPlan, isSort2ndProject)

          // generate the new sql with materialized view,
          // expIdMap is used if the query is a subquery, eg,
          // select sub.c1 from (select c1 from t1) sub
          // expId of c1 in subquery can't be updated, or the query will be failed
          matchedResultSqls :+= MVPlanRewriteRules.generateMatchedResultSql(queryOutputs,
            queryTableAliasMap, viewAttAliasMap, viewEC, compensateTables, compensateJoinPreds,
            queryCompensates, groupByCompensates, orders, materializedView)

          if (MATCH_POLICY_ONCE.equals(analyzer.getSqlConf.materializedViewMatchPolicy)) {
            loop.break
          } else if (matchedResultSqls.size >=
              analyzer.getSqlConf.materializedViewMultiplePolicyLimit) {
            loop.break
          }
        } catch {
          case ex: Exception =>
            logWarning(s"Optimize with materialized view ${materializedView.getFullName} failed," +
              s" ${ex.getMessage}")
        }
      }
    }

    if (matchedResultSqls.isEmpty) {
      // there is no materialized view matched, return the origin logical plan
      originPlan
    } else {
      var matchedResultSql = matchedResultSqls(0)
      // for multiple materialized views matched, pick the best according to cbo
      if (matchedResultSqls.size > 1) {
        for (i <- 1 to matchedResultSqls.size - 1) {
          matchedResultSql = matchedResultSql.compareTo(matchedResultSqls(i))
        }
      }
      // generate the optimized logical plan with the picked materialized view
      val optimizedPlan = MVPlanRewriteRules.generateNewPlan(analyzer,
        MVPlanRewriteRules.generateResultSql(matchedResultSql), matchedResultSql.exprIdMap)
      logInfo(s"Successfully optimize with materialized view: " +
        s"${matchedResultSql.materializedView.getFullName}")
      optimizedPlan
    }
  }

  private def splitPlanWithSort(queryPlan: LogicalPlan): (LogicalPlan, Seq[SortOrder], Boolean) = {
    queryPlan match {
      case Project(_, Sort(order, _, p: Project)) => (p, order, true)
      case Project(_, Sort(order, _, a: Aggregate)) => (a, order, true)
      case plan@(Project(_, _) | Aggregate(_, _, _)) => (plan, Seq.empty[SortOrder], false)
      case Sort(order, _, p: Project) => (p, order, false)
      case Sort(order, _, a: Aggregate) => (a, order, false)
      case _ => throw new TryMaterializedFailedException("find invalid plan in splitPlanWithSort")
    }
  }

  private def getMaterializedViews(): Seq[MaterializedView] = {
    val catalogTables = analyzer.getCatalogManager.v1SessionCatalog.getAllMaterializedViews(
      analyzer.getSqlConf.materializedViewDbs)
    val ss = SparkSession.active
    var materializedViews = Seq.empty[MaterializedView]
    catalogTables.foreach(catalogTable => {
      try {
        val unresolvedPlan = ss.sessionState.sqlParser.parsePlan(catalogTable.viewText.get)
        val prefixName = if (catalogTable.identifier.database.isDefined) {
          Seq(catalogTable.identifier.database.get)
        } else {
          Seq.empty[String]
        }
        val analyzed = analyzer.execute(unresolvedPlan)
        analyzer.checkAnalysis(analyzed)
        materializedViews :+= new MaterializedView(
          catalogTable.identifier.table, prefixName,
          analyzed)
      } catch {
        case ex: Exception =>
          logWarning(s"Error happened when parse MaterializedView ${catalogTable.identifier}," +
            s" ${ex.getMessage}")
      }
    })
    materializedViews
  }

  private def updateEquivalenceClass(
      ec: EquivalenceClasses,
      equiColumnsPreds: Seq[EqualTo],
      tableAliasMap: Map[String, String]): Unit = {
    for (pred <- equiColumnsPreds) {
      ec.addEquivalenceClass(
        getExpressionSql(pred.left, tableAliasMap),
        getExpressionSql(pred.right, tableAliasMap))
    }
  }

  // support the materialized view in the following format:
  // 1. select * from * where * group by * => Aggregate(_, _, _)
  // 2. select * from * where * => Project(_, _)
  private def checkMvPlan(plan: LogicalPlan): Unit = {
    plan.foreach {
      case Project(_, _) | Aggregate(_, _, _) =>
      case Filter(_, _) =>
      case Join(_, _, Inner, _, _) =>
      case HiveTableRelation(_, _, _, _, _) | LogicalRelation(_, _, _, _) =>
      case SubqueryAlias(_, Project(_, _)) | SubqueryAlias(_, Aggregate(_, _, _)) =>
        throw new TryMaterializedFailedException(
          "checkMvPlan: Subquery is unsupported in materialized view")
      case SubqueryAlias(_, _) =>
      case e@_ => throw new TryMaterializedFailedException(
        s"checkMvPlan: unsupported plan, ${e.getClass.getCanonicalName}")
    }
  }

  private def isValidForMaterialized(plan: LogicalPlan): Boolean = {
    var result = true
    plan.foreach {
      case Project(_, _) | Aggregate(_, _, _) =>
      case Filter(_, _) =>
      case Sort(_, _, _) =>
      case Join(_, _, Inner, _, _) =>
      case HiveTableRelation(_, _, _, _, _) | LogicalRelation(_, _, _, _) =>
      case SubqueryAlias(_, Project(_, _)) | SubqueryAlias(_, Aggregate(_, _, _)) =>
        result = false
      case SubqueryAlias(_, _) =>
      case op@_ =>
        result = false
    }
    result
  }

  private def getAttRefMap(
      project: LogicalPlan,
      tableAliasMap: Map[String, String],
      isQuery: Boolean): (Map[String, Expression], Map[String, String]) = {
    var attRefMap = Map.empty[String, Expression]
    var attAliasMap = Map.empty[String, String]
    project.foreach {
      case p@Project(_, _) =>
        val (arm, aam, _) = getSpecificAttRefs(p, tableAliasMap, isQuery)
        attRefMap ++= arm
        attAliasMap ++= aam
      case a@Aggregate(_, _, _) =>
        val (arm, aam, _) = getSpecificAttRefs(a, tableAliasMap, isQuery)
        attRefMap ++= arm
        attAliasMap ++= aam
      case Filter(condition, _) =>
        val arm = getAttRefsInCondition(condition, tableAliasMap)
        // check the expression, ignore if duplicate with attributes in outputs
        for ((k, v) <- arm) {
          if (!attRefMap.contains(k)) {
            attRefMap += (k -> v)
          }
        }
      case Join(_, _, _, condition, _) =>
        if (condition.isDefined) {
          val arm = getAttRefsInCondition(condition.get, tableAliasMap)
          // check the expression, ignore if duplicate with attributes in outputs
          for ((k, v) <- arm) {
            if (!attRefMap.contains(k)) {
              attRefMap += (k -> v)
            }
          }
        }
      case _ =>
    }

    (attRefMap, attAliasMap)
  }

  private def getAttRefsInCondition(
      condition: Expression, tableAliasMap: Map[String, String]): Map[String, Expression] = {
    var attRefMap = Map.empty[String, Expression]
    condition.foreach {
      case ar@AttributeReference(_, _, _, _) =>
        val fullName = getExpressionSql(ar, tableAliasMap)
        attRefMap += (fullName -> ar)
      case _ =>
    }

    attRefMap
  }

  private def getSpecificAttRefs(
      plan: LogicalPlan,
      tableAliasMap: Map[String, String],
      isQuery: Boolean): (Map[String, Expression], Map[String, String], Set[Expression]) = {
    var attRefMap = Map.empty[String, Expression]
    var attAliasMap = Map.empty[String, String]
    var groupByAttRefs = Set.empty[Expression]
    plan match {
      case Project(pl, _) =>
        val res = getAttRefsFromNameExpression(pl, tableAliasMap, isQuery)
        attRefMap = res._1
        attAliasMap = res._2
      case Aggregate(ges, aes, _) =>
        val res = getAttRefsFromNameExpression(aes, tableAliasMap, isQuery)
        attRefMap = res._1
        attAliasMap = res._2
        ges.foreach { ge =>
          groupByAttRefs += ge
        }
      case _ =>
    }
    (attRefMap, attAliasMap, groupByAttRefs)
  }

  private def getAttRefsFromNameExpression(
      nameExpressions: Seq[NamedExpression],
      tableAliasMap: Map[String, String],
      isQuery: Boolean): (Map[String, Expression], Map[String, String]) = {
    var attRefMap = Map.empty[String, Expression]
    var attAliasMap = Map.empty[String, String]
    nameExpressions.foreach {
      case ar@AttributeReference(name, _, _, _) =>
        val fullName = getExpressionSql(ar, tableAliasMap)
        attRefMap += (fullName -> ar)
        if (!attAliasMap.contains(fullName)) {
          attAliasMap += (fullName -> name)
        }
      case Alias(child, aliasName) =>
        if (!isQuery) {
          checkAliasName(aliasName)
        }
        val fullName = getExpressionSql(child, tableAliasMap)
        attRefMap += (fullName -> child)
        attAliasMap += (fullName -> aliasName)
      // shouldn't be here
      case _ =>
    }

    (attRefMap, attAliasMap)
  }

  private def checkAliasName(aliasName: String): Unit = {
    if (!aliasName.matches("[a-zA-Z0-9_]+")) {
      throw new TryMaterializedFailedException(s"Invalid alias name: $aliasName")
    }
  }

  private def getTableIdentifies(plan: LogicalPlan): (Map[String, String], Set[String]) = {
    var tableAliasMap = Map.empty[String, String]
    var tableSet = Set.empty[String]
    plan.foreach {
      case SubqueryAlias(name, SubqueryAlias(n, _)) =>
        tableAliasMap += (name.toString -> n.toString)
      case SubqueryAlias(name, _) =>
        tableAliasMap += (name.toString -> name.toString)
        if (tableSet.contains(name.toString)) {
          throw new TryMaterializedFailedException("Same table in from clause is not supported")
        }
        tableSet += name.toString
      case _ =>
    }
    (tableAliasMap, tableSet)
  }

  private def extractPredicates(plan: LogicalPlan): Seq[Expression] = {
    plan.collect[Seq[Expression]] {
      case Filter(condition, _) => extractPredicatesWithcondition(condition)
      case Join(_, _, _, condition, _) if (condition.isDefined) =>
        extractPredicatesWithcondition(condition.get)
      case _ => Seq.empty
    }.flatten
  }

  private def getFilterCondition(plan: LogicalPlan): Option[Expression] = {
    plan collectFirst {
      case Filter(condition, _) => condition
    }
  }

  private def extractPredicatesWithcondition(condition: Expression): Seq[Expression] = {
    condition match {
      case And(left, right) =>
        extractPredicatesWithcondition(left) ++ extractPredicatesWithcondition(right)
      case Or(left, right) =>
        extractPredicatesWithcondition(left) ++ extractPredicatesWithcondition(right)
      case Not(child) =>
        extractPredicatesWithcondition(child)
      case expr@_ => expr :: Nil
    }
  }

  // get column equality predicates
  private def splitColEqualPredicates(predicates: Seq[Expression]): Seq[EqualTo] = {
    var colEqualPreds = Seq.empty[EqualTo]
    var candicates: Map[Literal, AttributeReference] = Map.empty
    for (predicate <- predicates) {
      predicate match {
        case equalTo@EqualTo(l: AttributeReference, r: AttributeReference) =>
          colEqualPreds :+= equalTo
        case EqualTo(left: AttributeReference, right: Literal) =>
          candicates.get(right) match {
            case Some(ar) => colEqualPreds :+= EqualTo(ar, left)
            case None => candicates += (right -> left)
          }
        case EqualTo(left: Literal, right: AttributeReference) =>
          candicates.get(left) match {
            case Some(ar) => colEqualPreds :+= EqualTo(ar, left)
            case None => candicates += (left -> right)
          }
        case _ =>
      }
    }
    colEqualPreds
  }

  private def compensateViewPartial(
      queryTableSet: Set[String],
      viewTableSet: Set[String],
      queryEquiColumnsPreds: Seq[EqualTo],
      queryTableAliasMap: Map[String, String]): (Set[String], Seq[EqualTo]) = {
    val missedTableSet =
      queryTableSet.filter(qt => !viewTableSet.contains(qt))
    val newViewEquiColumnsPreds = mutable.Set.empty[EqualTo]

    for (mt <- missedTableSet) {
      newViewEquiColumnsPreds ++=
        queryEquiColumnsPreds.filter(
          p => isCompensateJoinCondition(mt, p, queryTableAliasMap))
    }
    (missedTableSet, Seq(newViewEquiColumnsPreds.toArray: _*))
  }

  private def isCompensateJoinCondition(
      tableName: String,
      exp: Expression,
      tableAliasMap: Map[String, String]): Boolean = {
    exp match {
      case EqualTo(l: AttributeReference, r: AttributeReference) =>
        tableAliasMap.getOrElse(l.qualifier.mkString("."), "").equals(tableName) ||
          tableAliasMap.getOrElse(r.qualifier.mkString("."), "").equals(tableName)
      case _ => false
    }
  }
}

class MaterializedView(val name: String, val prefixName: Seq[String], val plan: LogicalPlan) {
  def getFullName: String = (prefixName :+ name).mkString(".")
}
