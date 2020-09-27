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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.IdentifierWithDatabase
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryComparison, Cast, EqualTo, Expression, ExprId, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Complete, Count, Kurtosis, Max, Min, Skewness, StddevPop, StddevSamp, Sum, VariancePop, VarianceSamp}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, Sort}
import org.apache.spark.sql.execution.mv.MVCompatibleRules.getExpressionSql
import org.apache.spark.sql.types.IntegerType

object MVPlanRewriteRules {

  def generateResultSql(matchedResultSql: MatchedResultSql): String = {
    // generate result sql with materialized view
    val sb = StringBuilder.newBuilder
    sb.append("select " + matchedResultSql.outputs.mkString(",") +
      " from " + matchedResultSql.tables.mkString(","))

    if (!matchedResultSql.joinStr.isEmpty) {
      sb.append(" where " + matchedResultSql.joinStr)
      if (!matchedResultSql.filters.isEmpty) {
        sb.append(" and " + matchedResultSql.filters.mkString(" or "))
      }
    } else {
      if (!matchedResultSql.filters.isEmpty) {
        sb.append(" where " + matchedResultSql.filters.mkString(" or "))
      }
    }

    if (!matchedResultSql.groupByExprs.isEmpty) {
      sb.append(" group by " + matchedResultSql.groupByExprs.mkString(","))
    }

    if (!matchedResultSql.orderStrs.isEmpty) {
      sb.append(" order by " + matchedResultSql.orderStrs.mkString(","))
    }

    sb.toString
  }

  // generate the matched sql with materialized view,
  // expIdMap in MatchedResultSql is used if the query is a subquery, eg,
  // select sub.c1 from (select c1 from t1) sub
  // expId of c1 in subquery can't be updated, or the query will be failed
  def generateMatchedResultSql(
      queryOutputs: Seq[NamedExpression],
      queryTableAliasMap: Map[String, String],
      viewAttAliasMap: Map[String, String],
      viewEC: EquivalenceClasses,
      compensateTables: Set[String],
      compensateJoinPreds: Seq[EqualTo],
      finalCompensatePreds: Seq[Expression],
      groupByCompensates: Set[Expression],
      orders: Seq[SortOrder],
      materializedView: MaterializedView): MatchedResultSql = {

    // replace the expression with materialized view and get the output list for result
    val (outputs, exprIdMap) = generateOutputStrList(
      queryOutputs, queryTableAliasMap, viewAttAliasMap, viewEC, groupByCompensates,
      materializedView.getFullName, compensateTables)
    if (outputs.isEmpty) {
      throw new TryMaterializedFailedException("generateOutputStrList is empty")
    }

    val tables = Seq(materializedView.getFullName) ++
      getTableNamesWithAlias(compensateTables, queryTableAliasMap)

    // for join situation, query with the condition as t1.c1 = t2.c2
    // it should be filtered if view has this already
    var filteredCompensates = Seq.empty[Expression]
    finalCompensatePreds.foreach(preds => {
      val (isKeep, newExpr) =
        MaterializedViewUtil.filterConditions(preds, queryTableAliasMap, compensateTables)
      if (isKeep) {
        filteredCompensates :+= newExpr
      }
    })

    // replace the expression with materialized view and get the filters for result
    val updatedPreds = filteredCompensates.map(
      expr => expr match {
        case bc@BinaryComparison(left, right) =>
          bc.makeCopy(Array(getExpressionWithUpdatedAttRef(left, queryTableAliasMap,
            viewAttAliasMap, viewEC, materializedView.getFullName, false, compensateTables),
            getExpressionWithUpdatedAttRef(right, queryTableAliasMap,
              viewAttAliasMap, viewEC, materializedView.getFullName, false, compensateTables)))
        case _ => getExpressionWithUpdatedAttRef(expr, queryTableAliasMap,
          viewAttAliasMap, viewEC, materializedView.getFullName, false, compensateTables)
      }
    )

    // replace the expression with materialized view and get the join expression for result
    val updatedJoinPreds = compensateJoinPreds.map(
      expr => expr match {
        case bc@BinaryComparison(left, right) =>
          bc.makeCopy(Array(getExpressionWithUpdatedAttRef(left, queryTableAliasMap,
            viewAttAliasMap, viewEC, materializedView.getFullName, false, compensateTables),
            getExpressionWithUpdatedAttRef(right, queryTableAliasMap,
              viewAttAliasMap, viewEC, materializedView.getFullName, false, compensateTables)))
        case _ => getExpressionWithUpdatedAttRef(expr, queryTableAliasMap,
          viewAttAliasMap, viewEC, materializedView.getFullName, false, compensateTables)
      }
    )

    var filters = Seq.empty[String]
    for (pred <- updatedPreds) {
      filters :+= pred.sql
    }

    // replace the expression with materialized view and get group by expression for result
    val updatedGroupByAttRefs = groupByCompensates.map(
      getExpressionWithUpdatedAttRef(_, queryTableAliasMap,
        viewAttAliasMap, viewEC, materializedView.getFullName, false, compensateTables)
    )

    var groupByExprs = Seq.empty[String]
    for (expr <- updatedGroupByAttRefs) {
      groupByExprs :+= expr.sql
    }

    // replace the expression with materialized view and get order by expression for result
    var orderStrs = Seq.empty[String]
    val updatedOrders = orders.map(
      order => order.makeCopy(Array(getExpressionWithUpdatedAttRef(order.child, queryTableAliasMap,
        viewAttAliasMap, viewEC, materializedView.getFullName, false, compensateTables),
        order.direction, order.nullOrdering, order.sameOrderExpressions))
    )
    for (order <- updatedOrders) {
      orderStrs :+= order.sql
    }

    val joinStr = if (!updatedJoinPreds.isEmpty) {
      MaterializedViewUtil.mergeToAnd(updatedJoinPreds).sql
    } else {
      ""
    }

    new MatchedResultSql(outputs, tables, joinStr, filters, groupByExprs, orderStrs,
      exprIdMap, materializedView)
  }

  def generateNewPlan(
      analyzer: Analyzer,
      sql: String,
      expIdMap: Map[String, ExprId]): LogicalPlan = {
    val unresolved = SparkSession.active.sessionState.sqlParser.parsePlan(sql)
    val resolved = analyzer.execute(unresolved)
    analyzer.checkAnalysis(resolved)
    recoverExprId(resolved, expIdMap)
  }

  // when rewrite the plan of subquery
  // the value of exprId which is referenced from outer should not be changed
  // eg, select a.id from (select t.id from t) a
  // the exprId of "a.id" is the same as "t.id" in subquery, it should be the same after rewrite
  private def recoverExprId(
      newPlan: LogicalPlan,
      exprIdMap: Map[String, ExprId]): LogicalPlan = {
    newPlan.transform {
      case Project(projectList, child) =>
        Project(updateOutputsWithExprId(projectList, exprIdMap), child)
      case Aggregate(groupExprs, aggExprs, child) =>
        Aggregate(groupExprs, updateOutputsWithExprId(aggExprs, exprIdMap), child)
      case other@_ => other
    }
  }

  private def updateOutputsWithExprId(
      namedExprs: Seq[NamedExpression],
      exprIdMap: Map[String, ExprId]): Seq[NamedExpression] = {
    var updatedOutputs = Seq.empty[NamedExpression]
    namedExprs.foreach {
      namedExpr => namedExpr match {
        case alias@Alias(child, name) =>
          val rawExprId = exprIdMap.getOrElse(name,
            throw new TryMaterializedFailedException(
              s"can't recover the exprid for alias:${alias.sql}"))
          updatedOutputs :+= Alias(child, name)(exprId = rawExprId, qualifier = alias.qualifier,
            explicitMetadata = alias.explicitMetadata)
        case ar: AttributeReference =>
          // all outputs in result should be Alias, but with order clause,
          // the AttributeReference maybe exist in output
          if (ar.qualifier.isEmpty) {
            val rawExprId = exprIdMap.getOrElse(ar.name,
              throw new TryMaterializedFailedException(
                s"can't recover the exprid for AttributeReference:${ar.sql}"))
            updatedOutputs :+= ar.withExprId(rawExprId)
          } else {
            updatedOutputs :+= ar
          }
        case other@_ =>
          throw new TryMaterializedFailedException(s"can't recover the exprid for:${other.sql}")
      }
    }
    updatedOutputs
  }

  private def getTableFullName(
      ident: IdentifierWithDatabase,
      queryTableAliasMap: Map[String, String]): String = {
    val tableName = queryTableAliasMap.getOrElse(ident.unquotedString, "")
    if (tableName.equals(ident.unquotedString)) {
      tableName
    } else {
      tableName + " " + ident.unquotedString
    }
  }

  // for compensate tables, get the name with alias if necessary
  // query: select * from mv_db.t1, db1.t2, db1.t3 as t
  // in above query, compensate tables are db1.t2 and db1.t3
  // tableNames = ["db1.t2", "db1.t3"]
  // check if tableName has alias and return the table name used in from clause
  // output: ["db1.t2", "db1.t3 t"]
  private def getTableNamesWithAlias(
      tableNames: Set[String],
      queryTableAliasMap: Map[String, String]): Set[String] = {
    tableNames.map { tname =>
      val aliasSet = queryTableAliasMap.filter(
        entry => entry._2.equals(tname) && !entry._1.equals(tname)).keySet
      if (aliasSet.isEmpty) {
        tname
      } else {
        tname + " " + aliasSet.head
      }
    }
  }

  def filterOutputList(
      queryPlan: LogicalPlan,
      isSort2ndProject: Boolean): Seq[NamedExpression] = {
    var queryOutputs = Seq.empty[NamedExpression]
    queryPlan match {
      case Project(pl, _) =>
        pl.foreach(exp => exp match {
          case ar: AttributeReference =>
            if (!isSort2ndProject) {
              queryOutputs :+= ar
            }
          case other => queryOutputs :+= other
        })
      case Aggregate(_, aes, _) => queryOutputs = aes
      case _ => throw new TryMaterializedFailedException("Empty outputs to generate replaced sql")
    }
    queryOutputs
  }

  private def generateOutputStrList(
      queryOutputs: Seq[NamedExpression],
      queryTableAliasMap: Map[String, String],
      viewAttAliasMap: Map[String, String],
      viewEC: EquivalenceClasses,
      groupByCompensates: Set[Expression],
      viewName: String,
      compensateTableNames: Set[String]): (Seq[String], Map[String, ExprId]) = {
    var projects = Seq.empty[String]
    // track the origin exprId of all outputs and use it to recover final logic plan
    var exprIdMap = Map.empty[String, ExprId]

    val newProjectList = queryOutputs.map(
      expr => {
        val updateExpr = getExpressionWithUpdatedAttRef(expr, queryTableAliasMap,
          viewAttAliasMap, viewEC, viewName, !groupByCompensates.isEmpty, compensateTableNames)
        updateExpr match {
          case ar@AttributeReference(name, _, _, _) =>
            exprIdMap += (name -> ar.exprId)
            Alias(ar, name)()
          case alias@Alias(_, name) =>
            exprIdMap += (name -> alias.exprId)
            alias
          case _ => updateExpr
        }
      }
    )

    newProjectList.foreach(projects :+= _.sql)

    (projects, exprIdMap)
  }

  private def getExpressionWithUpdatedAttRef(
      queryExpr: Expression,
      queryTableAliasMap: Map[String, String],
      viewAttAliasMap: Map[String, String],
      viewEC: EquivalenceClasses,
      viewName: String,
      hasGroupCompensates: Boolean,
      compensateTableNames: Set[String]): Expression = {
    var newExpr: Expression = null

    // literal doesn't need transform
    if (!shouldTrans(queryExpr)) {
      return queryExpr
    }

    newExpr = queryExpr transform {
      case expr@_ if (!getViewColumnForReplace(
        expr, queryTableAliasMap, viewAttAliasMap, viewEC).isEmpty) =>
        if (hasGroupCompensates && expr.isInstanceOf[AggregateExpression]) {
          // Deal with the situation:
          // in mv: select count(a) as c ........ group by c1, c2
          // in query: select count(a) .......... group by c1
          //   result: select sum(mv.c) .......... group by c1
          // note: distinct is not supported in mv, eg:
          // in mv: select count(distinct a) as c ........ group by c1, c2
          // in query: select count(distinct a) .......... group by c1
          // result should be: select count(distinct a) .......... group by c1
          // here is the problem, we need a in mv's output, but it should be a wrong case in mv
          // select a, count(distinct a) as c ...... group by ...
          // don't transform the distinct function and the process will fail in the following check
          val aggExpr = expr.asInstanceOf[AggregateExpression]
          if (aggExpr.isDistinct) {
            aggExpr
          } else {
            val newAttRef = AttributeReference(getColumnName(viewAttAliasMap(
              getExpressionSql(aggExpr, queryTableAliasMap))),
              IntegerType)(qualifier = viewName.split("\\.").toSeq)
            transformAggrExpression(aggExpr, newAttRef)
          }
        } else {
          val exprId = expr match {
            case ne: NamedExpression => ne.exprId
            case _ => NamedExpression.newExprId
          }

          val newColumnName = getViewColumnForReplace(
            expr, queryTableAliasMap, viewAttAliasMap, viewEC)
          AttributeReference(newColumnName,
            IntegerType)(exprId = exprId, qualifier = viewName.split("\\.").toSeq)
        }
    }

    // if there is no transform with attribute exist,
    // and the attribute is not in compensate table, something wrong
    if (newExpr.sql.equals(queryExpr.sql) && isAttRefExist(queryExpr) &&
      !isAllCompensateAttRefs(queryExpr, compensateTableNames, queryTableAliasMap)) {
      throw new TryMaterializedFailedException(
        s"generateProjectStrList: expr without transform, ${queryExpr.sql}")
    }

    newExpr
  }

  // currently support count, sum, min, max
  private def transformAggrExpression(
      expr: AggregateExpression,
      newAttRef: AttributeReference): Expression = {
    expr match {
      case AggregateExpression(s@Count(_), _, isDistinct, _, _) =>
        AggregateExpression(Sum(newAttRef), Complete, isDistinct)
      case AggregateExpression(s@Sum(_), _, isDistinct, _, _) =>
        val newSum = s.makeCopy(Array(newAttRef))
        AggregateExpression(newSum.asInstanceOf[Sum], Complete, isDistinct)
      case AggregateExpression(s@Min(_), _, isDistinct, _, _) =>
        val newMin = s.makeCopy(Array(newAttRef))
        AggregateExpression(newMin.asInstanceOf[Min], Complete, isDistinct)
      case AggregateExpression(s@Max(_), _, isDistinct, _, _) =>
        val newMax = s.makeCopy(Array(newAttRef))
        AggregateExpression(newMax.asInstanceOf[Max], Complete, isDistinct)
      case _ => expr
    }
  }

  // check materialized view's output to find a replaced column
  // if find it in EquivalenceClassesMap, pick anyone
  private def getViewColumnForReplace(
      queryExpr: Expression,
      queryTableAliasMap: Map[String, String],
      viewAttAliasMap: Map[String, String],
      viewEC: EquivalenceClasses): String = {
    val queryExprSql = getExpressionSql(queryExpr, queryTableAliasMap)
    var replacedFullName = ""
    if (!viewAttAliasMap.contains(queryExprSql)) {
      // can't find the same output's name of query, try to find it in EquivalenceClassesMap
      val equalColumns = viewEC.getEquivalenceClassesMap.getOrElse(
        queryExprSql, Set.empty[String]).filter(fn => viewAttAliasMap.contains(fn)).toSeq
      if (!equalColumns.isEmpty) {
        // get the valid columns in EquivalenceClassesMap, pick anyone of them
        replacedFullName = equalColumns(0)
      }
    } else {
      // query and materialized view has the same name of output
      replacedFullName = queryExprSql
    }

    getColumnName(viewAttAliasMap.getOrElse(replacedFullName, ""))
  }

  private def isAllCompensateAttRefs(
      expr: Expression,
      compensateTableNames: Set[String],
      queryTableAliasMap: Map[String, String]): Boolean = {
    var result = true
    expr.foreach {
      case ar: AttributeReference =>
        val tname = queryTableAliasMap.getOrElse(ar.qualifier.mkString("."), "")
        if (!compensateTableNames.contains(tname)) {
          result = false
        }
      case _ =>
    }
    result
  }

  private def getColumnName(fullName: String): String = {
    val colNamesPats = fullName.split("\\.")
    colNamesPats(colNamesPats.size - 1)
  }

  private def shouldTrans(expr: Expression): Boolean = {
    expr match {
      case l: Literal => false
      case Cast(Literal(_, _), _, _) => false
      case Cast(Cast(Literal(_, _), _, _), _, _) => false
      case Cast(Cast(Cast(Literal(_, _), _, _), _, _), _, _) => false
      case _ => true
    }
  }

  private def isAttRefExist(expr: Expression): Boolean = {
    var result = false
    expr.foreach {
      case AttributeReference(_, _, _, _) => result = true
      case _ =>
    }
    result
  }
}

class MatchedResultSql(
    val outputs: Seq[String],
    val tables: Seq[String],
    val joinStr: String,
    val filters: Seq[String],
    val groupByExprs: Seq[String],
    val orderStrs: Seq[String],
    val exprIdMap: Map[String, ExprId],
    val materializedView: MaterializedView) {

  // Compare result sql according to shuffle & computation
  // it's an initial version, and can be improved with table's statics
  def compareTo(otherResultSql: MatchedResultSql): MatchedResultSql = {
    var score = 0

    // compare table/groupBy size, more size means more shuffle, eg,
    score += (otherResultSql.tables.length - tables.length) +
      (otherResultSql.groupByExprs.length - groupByExprs.length)

    if (score == 0) {
      // if there is no different with shuffle, compare filter, less filter means less computation
      score += otherResultSql.filters.length - filters.length
    }

    if (score >= 0) {
      this
    } else {
      otherResultSql
    }
  }
}
