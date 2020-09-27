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

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BinaryComparison, Cast, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Not, Or, SortOrder}
import org.apache.spark.sql.types.DataType

object MVCompatibleRules {

  def checkJoinCondition(
      colEqualPreds: Seq[EqualTo],
      tableSet: Set[String],
      tableAliasMap: Map[String, String]): Unit = {
    val tableN = tableSet.size
    if (tableN > 1) {
      // doesn't support full join
      if (colEqualPreds.size < tableN - 1) {
        throw new TryMaterializedFailedException(
          "checkJoinCondition failed, need more col equal condition")
      }
      val tables = tableSet.toSeq
      val ec = new EquivalenceClasses
      // for table1...tableN, create EquivalenceClasses with colEqualPreds, eg,
      // tables: t1, t2, t3
      // [support] colEqualPreds1: (t1.c1 = t2.c1, t1.c1 = t3.c1) => EC (t1, (t1, t2, t3))
      // [unSupport] colEqualPreds2: (t1.c1 = t2.c1, t1.c2 = t2.c2) => EC (t1, (t1, t2))
      for (i <- 0 to tableN - 2) {
        for (j <- i + 1 to tableN - 1) {
          colEqualPreds.foreach {
            eq => eq match {
              case EqualTo(l: AttributeReference, r: AttributeReference) =>
                if ((tableAliasMap.getOrElse(l.qualifier.mkString("."), "l").equals(tables(i))
                  && tableAliasMap.getOrElse(r.qualifier.mkString("."), "r").equals(tables(j)))
                  || (tableAliasMap.getOrElse(r.qualifier.mkString("."), "r").equals(tables(i))
                  && tableAliasMap.getOrElse(l.qualifier.mkString("."), "l").equals(tables(j)))) {
                  ec.addEquivalenceClass(tables(i), tables(j))
                }
              case _ => throw new TryMaterializedFailedException(
                "checkJoinCondition failed, unexpected colEqualPreds")
            }
          }
        }
      }

      if (ec.getEquivalenceClassesMap.size == 0
        || ec.getEquivalenceClassesMap.getOrElse(tables(0), Set.empty[String]).size != tableN) {
        throw new TryMaterializedFailedException(
          "checkJoinCondition failed, full join is not supported")
      }
    }
  }

  // check is project list match, view's outputs + viewMissedIdentifies >= query's outputs
  def checkProjectListMatch(
      queryProjectAttRefMap: Map[String, Expression],
      viewProjectAttRefMap: Map[String, Expression],
      compensateTables: Set[String],
      queryEC: EquivalenceClasses,
      viewEC: EquivalenceClasses,
      queryTableAliasMap: Map[String, String]): Unit = {

    queryProjectAttRefMap.values.foreach(expr => {
      var matched = false
      var hasAttRef = false
      var hasLiteral = false
      val qExprSql = getExpressionSql(expr, queryTableAliasMap)
      if (!viewProjectAttRefMap.contains(qExprSql)) {
        expr.foreach {
          case ar@AttributeReference(_, _, _, _) =>
            val qAttRefSql = getExpressionSql(ar, queryTableAliasMap)
            if (viewProjectAttRefMap.contains(qAttRefSql) ||
              // if att in the compensate table?
              MaterializedViewUtil.isAttInTables(ar, compensateTables, queryTableAliasMap) ||
              // if the query output can be evaluated by another equal column in view?
              // it aways happened in join situation
              queryEC.getEquivalenceClassesMap.getOrElse(
                qAttRefSql, Set.empty[String]).filter(
                att => viewProjectAttRefMap.contains(att)).size > 0) {
              matched = true
            }
            hasAttRef = true
          case l: Literal => hasLiteral = true
          case _ =>
        }
      } else {
        matched = true
      }

      // 2nd chance: expression with literal and without attribute
      if (hasLiteral && !hasAttRef) {
        matched = true
      }

      // check as false with whole expression sql compare
      // has the 3rd chance to check if the sub expression of others
      if (!matched) {
        viewProjectAttRefMap.values.foreach(vexpr => {
          if (qExprSql.contains(getExpressionSql(vexpr, queryTableAliasMap))) {
            matched = true
          }
        })
      }
      if (!matched) {
        throw new TryMaterializedFailedException("Output/Order list is not satisfied")
      }
    })
  }

  // check if order is satisfied with the materialized view
  def checkOrdersMatch(
      orders: Seq[SortOrder],
      viewProjectAttRefMap: Map[String, Expression],
      compensateTables: Set[String],
      queryEC: EquivalenceClasses,
      viewEC: EquivalenceClasses,
      queryTableAliasMap: Map[String, String]): Unit = {

    if (!orders.isEmpty) {
      var orderExprMap = Map.empty[String, Expression]
      orders.foreach(order => {
        order.child match {
          case ar: AttributeReference =>
          case _ => throw new TryMaterializedFailedException(
            "Only AttributeReference is supported in order list")
        }
        orderExprMap += (getExpressionSql(order.child, queryTableAliasMap) -> order.child)
      })
      checkProjectListMatch(orderExprMap, viewProjectAttRefMap,
        compensateTables, queryEC, viewEC, queryTableAliasMap)
    }
  }

  // check if group by is satisfied with the materialized view
  // if satisfied, the result group by expression will be:
  // 1. empty (expression in query is the same as in materialized view)
  // 2. non-empty (depend on the result of expression match)
  def isGroupByAttRefsMatch(
      queryGroupByExprs: Set[Expression],
      viewGroupByExprs: Set[Expression],
      compensateTables: Set[String],
      viewEC: EquivalenceClasses,
      queryTableAliasMap: Map[String, String],
      viewTableAliasMap: Map[String, String]): (Boolean, Set[Expression]) = {

    if (viewGroupByExprs.isEmpty) {
      if (queryGroupByExprs.isEmpty) {
        (true, Set.empty[Expression])
      } else {
        (true, queryGroupByExprs)
      }
    } else {
      if (queryGroupByExprs.isEmpty) {
        throw new TryMaterializedFailedException(
          "isGroupByAttRefsMatch: view has group by but query hasn't")
      } else {
        // every attribute used in query must be contained in materialized view,
        // for example:
        // materialized view: group by c1, c2, c3
        // query1: group by c1, c2 => satisfied
        // query2: group by c1, c2, c4 => unsatisfied
        var viewMatchedExprs = Set.empty[Expression]
        for (qgbExpr <- queryGroupByExprs) {
          var isMatch = false
          val qgbExprSql = getExpressionSql(qgbExpr, queryTableAliasMap)
          for (vgbExpr <- viewGroupByExprs) {
            val vgbExprSql = getExpressionSql(vgbExpr, viewTableAliasMap)
            if (qgbExprSql.equals(vgbExprSql) ||
              viewEC.getEquivalenceClassesMap.getOrElse(vgbExprSql,
                Set.empty[String]).contains(qgbExprSql)) {
              isMatch = true
              viewMatchedExprs += vgbExpr
            } else {
              qgbExpr match {
                case ar@AttributeReference(_, _, _, _)
                    if MaterializedViewUtil.isAttInTables(
                      ar, compensateTables, queryTableAliasMap) =>
                  isMatch = true
                case _ =>
              }
            }
          }
          if (!isMatch) {
            throw new TryMaterializedFailedException("isGroupByAttRefsMatch failed")
          }
        }
        if (viewMatchedExprs.size < viewGroupByExprs.size) {
          (true, queryGroupByExprs)
        } else {
          (true, Set.empty[Expression])
        }
      }
    }
  }

  // limitation: only suport 2 level filter: "Or-And"
  // unsupport: "And-Or", "Not"
  def computeCompensation(
      queryCondition: Option[Expression],
      viewCondition: Option[Expression],
      queryEC: EquivalenceClasses,
      viewEC: EquivalenceClasses,
      queryAttRefMap: Map[String, Expression],
      viewAttRefMap: Map[String, Expression],
      queryTableAliasMap: Map[String, String],
      viewTableAliasMap: Map[String, String]): (Seq[Expression], Seq[Expression]) = {

    // col equal preds will be ignored for validation
    val queryOrs = if (queryCondition.isDefined) {
      splitOr(queryCondition.get, queryTableAliasMap, Set.empty[String])
    } else {
      Seq.empty[Expression]
    }

    val viewOrs = if (viewCondition.isDefined) {
      splitOr(viewCondition.get, viewTableAliasMap, Set.empty[String])
    } else {
      Seq.empty[Expression]
    }
    if (queryOrs.isEmpty && viewOrs.isEmpty) {
      (Seq.empty[Expression], Seq.empty[Expression])
    } else if (queryOrs.isEmpty && !viewOrs.isEmpty) {
      throw new TryMaterializedFailedException(
        "computeCompensationOrs: query condition not satisfied")
    } else if (!queryOrs.isEmpty && viewOrs.isEmpty) {
      (queryOrs, Seq.empty[Expression])
    } else {
      var compensationOrs = Seq.empty[Expression]
      var unionCompensates = Seq.empty[Expression]
      for (qor <- queryOrs) {
        var exist = false
        var fullMatched = true
        val queryAnds = splitAnd(qor)
        var lastCompensates = Seq.empty[Expression]
        for (vor <- viewOrs) {
          val viewAnds = splitAnd(vor)
          val (matchPreds, compensates, uc) =
            computeCompensationAnds(queryAnds, viewAnds, queryEC, viewEC,
              queryAttRefMap, viewAttRefMap, queryTableAliasMap, viewTableAliasMap)
          if (matchPreds) {
            exist = true
            lastCompensates = compensates
            if (!compensates.isEmpty) {
              fullMatched = false
            }
          } else {
            fullMatched = false
          }
          unionCompensates ++= uc
        }
        if (exist) {
          if (viewOrs.size > 1 && !fullMatched) {
            compensationOrs :+= qor
          } else {
            if (!lastCompensates.isEmpty) {
              compensationOrs :+= MaterializedViewUtil.mergeToAnd(lastCompensates)
            }
          }
          unionCompensates = Seq.empty[Expression]
        } else {
          throw new TryMaterializedFailedException(
            s"computeCompensationOrs failed: unsatisfied expression, ${qor.sql}")
        }
      }
      (compensationOrs, unionCompensates)
    }
  }

  def getExpressionSql(expr: Expression, tableAliasMap: Map[String, String]): String = {
    expr match {
      case ar@AttributeReference(name, _, _, _) =>
        // empty qualifier for attributeReference, it may be from Alias with following places:
        // 1. order by <alias>
        // 2. select <alias> from ..... order by xxx
        if (ar.qualifier.isEmpty) {
          name
        } else {
          tableAliasMap.getOrElse(ar.qualifier.mkString("."), "") + "." + name
        }
      case _ => expr.sql
    }
  }

  private def splitAnd(expr: Expression): Seq[Expression] = {
    expr match {
      case And(left, right) => splitAnd(left) ++ splitAnd(right)
      case or: Or => throw new TryMaterializedFailedException(
        "Or expression inside And expression is Unsupported")
      case not: Not => throw new TryMaterializedFailedException(
        "Not expression is Unsupported")
      case _ => Seq(expr)
    }
  }

  private def splitOr(
      expr: Expression,
      tableAliasMap: Map[String, String],
      tnames: Set[String]): Seq[Expression] = {
    expr match {
      case Or(left, right) => splitOr(left, tableAliasMap, tnames) ++
        splitOr(right, tableAliasMap, tnames)
      case not: Not => throw new TryMaterializedFailedException(
        "splitOr: Not expression is unsupported")
      case _ =>
        val (isKeep, newExpr) =
          MaterializedViewUtil.filterConditions(expr, tableAliasMap, tnames)
        if (isKeep) {
          Seq(newExpr)
        } else {
          Seq.empty[Expression]
        }
    }
  }

  private def computeCompensationAnds(
      queryAnds: Seq[Expression],
      viewAnds: Seq[Expression],
      queryEC: EquivalenceClasses,
      viewEC: EquivalenceClasses,
      queryAttRefMap: Map[String, Expression],
      viewAttRefMap: Map[String, Expression],
      queryTableAliasMap: Map[String, String],
      viewTableAliasMap: Map[String, String]): (Boolean, Seq[Expression], Seq[Expression]) = {
    var result = true
    var compensationAnds = queryAnds.toSet
    var unionCompensations = Seq.empty[Expression]
    // Step 1: try match Expression Sql, following are examples situations:
    // a. query: [a = 1]
    //    view: [a = 1, b = 1]
    //    result: [true, [a = 1]]
    // b. query: [b = 1, a = 1]
    //    view: [a = 1, b = 1]
    //    result: [true, []]
    // c. query: [c = 1] with EC map (a = c)
    //    view: [a = 1, b = 1]
    //    result: [true, [c = 1]]
    // d. query: [a = 1, b = 1, c = 1]
    //    view: [a = 1, b = 1]
    //    result: [false, [maybe has value, but will be ignored]]
    // Step 2: Process BinaryComparison
    // limitation: has 1 attributReference and only support >, >=, =, <, <=
    // a. query: [a > 1]
    //    view: [a > 0]
    //    result: [true, [a > 1]]
    // b. query: [a > 1, a < 10]
    //    view: [a > 1, a < 11]
    //    result: [true, [a < 10]]
    // c. query: [a > 1, a < 10]
    //    view: [a > 1, a < 10]
    //    result: [true, []]
    for (vExpr <- viewAnds) {
      var matched = false
      // check every query filter expression with current view filter expression
      for (qExpr <- queryAnds) {
        // step1: check express depend on string of expression
        val sqlMatch = isExpressionSqlMatch(qExpr, vExpr, viewEC, viewAttRefMap, queryTableAliasMap)
        if (sqlMatch) {
          if (compensationAnds.contains(qExpr)) {
            compensationAnds -= qExpr
          }
        }

        val qAttRefs = getAttRefsInExpression(qExpr)
        val vAttRefs = getAttRefsInExpression(vExpr)
        if (qAttRefs.isEmpty) {
          if (sqlMatch) {
            matched = true
          }
        } else if (qAttRefs.size == 1) {
          if (vAttRefs.isEmpty) {
            // example:
            // qExpr: substr(c1, 1)
            // vExpr: 2 > 1
          } else if (vAttRefs.size == 1) {
            if (isAttributeReferenceMatch(qAttRefs(0), vAttRefs(0),
              viewEC, queryTableAliasMap, viewTableAliasMap)) {
              qExpr match {
                case bc: BinaryComparison =>
                  // step2: check expression depend on real value
                  val queryBC = getBinaryComparisons(Seq(qExpr), qAttRefs(0),
                    viewEC, queryTableAliasMap, viewTableAliasMap)((_, _, _, _, _) => true)
                  if (!queryBC.isEmpty) {
                    // example:
                    // queryAnds: [a = 2]
                    // viewAnds: [a > 1, a < 10]
                    // come here when process following expression pair:
                    // qExpr: [a = 2]
                    // vExpr: [a > 1]
                    // in getScopeCompensations, will expand viewAnds, and compare
                    // [a = 2] vs [a > 1, a < 10]
                    // not [a = 2] vs [a > 1]
                    val uc = getScopeCompensations(qAttRefs(0), queryBC(0), viewAnds,
                      viewEC, queryAttRefMap, queryTableAliasMap, viewTableAliasMap)

                    if (!uc.isEmpty) {
                      unionCompensations ++= uc
                    }
                    matched = true
                  } else {
                    // example:
                    // qExpr: abs(c1) > 0
                    // vExpr: abs(c1) > 1
                    if (sqlMatch) {
                      matched = true
                    }
                  }
                case _ =>
                  // example:
                  // qExpr: substr(c1, 1)
                  // vExpr: substr(c1, 2)
                  if (sqlMatch) {
                    matched = true
                  }
              }
            } else {
              // example:
              // qExpr: substr(c1, 1)
              // vExpr: substr(c2, 2)
            }
          } else {
            // example:
            // qExpr: substr(c1, 1)
            // vExpr: concat_ws(c1, c2)
          }
        } else {
          // for expression with 2 columns, only compare with expression.sql
          // example:
          // qExpr: concat_ws(c1, c2)
          // vExpr: Any
          if (sqlMatch) {
            matched = true
          }
        }
      }

      if (matched) {
        if (!unionCompensations.isEmpty) {
          // eg, view:  a > 1 and a < 10
          //    query:  a > 2 and a < 9
          // after calculation, unionCompensations will be [a>=10, a<=1]
          // need check all expression in unionCompensations are excluded
          matched = checkCompensationCondition(
            unionCompensations, queryTableAliasMap, queryAttRefMap)
        }
      }

      if (!matched) {
        result = false
      }

      unionCompensations = Seq.empty[Expression]
    }
    (result, compensationAnds.toSeq, unionCompensations)
  }

  private def isExpressionSqlMatch(
      qExpr: Expression,
      vExpr: Expression,
      viewEC: EquivalenceClasses,
      viewAttRefMap: Map[String, Expression],
      queryTableAliasMap: Map[String, String]): Boolean = {

    // limitation here, expression.canonicalized.sql can support limited functions, eg,
    // concat_ws(name, '_', '2') = 'name' won't match it self
    // expression.sql is order sensitive:
    // concat_ws(name, '_', '2') = 'name' won't match 'name' = concat_ws(name, '_', '2')
    if (qExpr.sql.equals(vExpr.sql) ||
      qExpr.canonicalized.sql.equals(vExpr.canonicalized.sql)) {
      return true
    }

    val attRefs = getAttRefsInExpression(qExpr)
    // limitation: if only one AttributeReference in expression, check it with EquivalenceClasses
    if (attRefs.size == 1) {
      val equalAttRefs = viewEC.getEquivalenceClassesMap.getOrElse(
        getExpressionSql(attRefs(0), queryTableAliasMap), Set.empty)
      if (!equalAttRefs.isEmpty) {
        for (ar <- equalAttRefs) {
          // get new equalTo with reference replace
          val newViewExpr = vExpr transform {
            case AttributeReference(_, _, _, _) => viewAttRefMap(ar)
          }
          // check with new expression whose attribute is replaced according to EC
          if (qExpr.sql.equals(newViewExpr.sql) ||
            qExpr.canonicalized.sql.endsWith(newViewExpr.canonicalized.sql)) {
            return true
          }
        }
      }
    }
    false
  }

  private def getAttRefsInExpression(expr: Expression): Seq[AttributeReference] = {
    var attRefs = Seq.empty[AttributeReference]
    expr.foreach {
      case ar@AttributeReference(_, _, _, _) => attRefs :+= ar
      case _ =>
    }
    attRefs
  }

  private def checkCompensationCondition(
      compensations: Seq[Expression],
      tableAliasMap: Map[String, String],
      attRefMap: Map[String, Expression]): Boolean = {
    var result = true
    var attRefBcMap = Map.empty[String, Seq[BinaryComparison]]
    compensations.foreach {
      // every expr is generated by valueScopesToBinaryComparison
      // only BinaryComparison(AttributeReference, _) pattern is checked, the others are invalid
      expr => expr match {
        case bc@BinaryComparison(ar: AttributeReference, _) =>
          val attRefFullName = getExpressionSql(ar, tableAliasMap)
          val attRefBcs = attRefBcMap.getOrElse(attRefFullName, Seq.empty[BinaryComparison]) :+ bc
          attRefBcMap += (attRefFullName -> attRefBcs)
        case _ => throw new TryMaterializedFailedException(
          s"unifyCompensationCondition got an invalid expression ${expr.sql}")
      }
    }

    for ((attRef, bcs) <- attRefBcMap) {
      if (bcs.size == 1) {
        result = false
      } else {
        if (!checkCompensateValueScopes(attRefMap(attRef).dataType, bcs)) {
          result = false
        }
      }
    }
    result
  }

  private def checkCompensateValueScopes(
      dataType: DataType,
      bcs: Seq[BinaryComparison]): Boolean = {
    var result = true
    val cvss = getColValueScopes(bcs)
    if (cvss.size == 0) {
      // it shouldn't be here
      throw new TryMaterializedFailedException(
        "getCompensateValueScopes got an invalid status")
    } else if (cvss.size == 1) {

    } else if (cvss.size > 1) {
      for (i <- 0 to cvss.size - 2) {
        val cvs1 = cvss(i)
        for (j <- i + 1 to cvss.size - 1) {
          val cvs2 = cvss(j)
          // todo: support union mv
          if (!MaterializedViewUtil.mergeColValueScope(dataType, cvs1, cvs2).isEmpty) {
            result = false
          }
        }
      }
    }
    result
  }

  private def getColValueScopes(bcs: Seq[BinaryComparison]): Seq[ColValueScope] = {
    bcs.map(bc => binaryComparisonToColValueScope(bc))
  }

  private def binaryComparisonToColValueScope(bc: BinaryComparison): ColValueScope = {
    bc match {
      case GreaterThan(_, Literal(value, _)) =>
        new ColValueScope(Some(value), None, false, false)
      case GreaterThan(Literal(value, _), _) =>
        new ColValueScope(None, Some(value), false, false)
      case GreaterThanOrEqual(_, Literal(value, _)) =>
        new ColValueScope(Some(value), None, true, false)
      case GreaterThanOrEqual(Literal(value, _), _) =>
        new ColValueScope(None, Some(value), false, true)
      case LessThan(_, Literal(value, _)) =>
        new ColValueScope(None, Some(value), false, false)
      case LessThan(Literal(value, _), _) =>
        new ColValueScope(Some(value), None, false, false)
      case LessThanOrEqual(_, Literal(value, _)) =>
        new ColValueScope(None, Some(value), false, true)
      case LessThanOrEqual(Literal(value, _), _) =>
        new ColValueScope(Some(value), None, true, false)
      case EqualTo(Literal(value, _), _) =>
        new ColValueScope(Some(value), Some(value), true, true)
      case EqualTo(_, Literal(value, _)) =>
        new ColValueScope(Some(value), Some(value), true, true)
      case _ =>
        throw new TryMaterializedFailedException(
          s"binaryComparisonToColValueScope: unexpected expression ${bc.sql}")
    }
  }

  private def getScopeCompensations(
      queryAttRef: AttributeReference,
      queryBC: BinaryComparison,
      viewAnds: Seq[Expression],
      viewEC: EquivalenceClasses,
      queryAttRefMap: Map[String, Expression],
      queryTableAliasMap: Map[String, String],
      viewTableAliasMap: Map[String, String]): Seq[Expression] = {
    val viewBCs = getBinaryComparisons(viewAnds, queryAttRef, viewEC,
      queryTableAliasMap, viewTableAliasMap)(isAttributeReferenceMatch)
    if (!viewBCs.isEmpty) {
      val queryColValueScope = getColValueScopes(Seq(queryBC))
      val mergedViewValueScope = mergeColValueScopes(queryAttRef, getColValueScopes(viewBCs))

      val compensateValueScopes: Seq[ColValueScope] =
        MaterializedViewUtil.getCompensateValueScopes(
          queryAttRefMap(getExpressionSql(queryAttRef, queryTableAliasMap)).dataType,
          queryColValueScope(0), mergedViewValueScope)
      valueScopesToBinaryComparison(queryAttRef, compensateValueScopes)
    } else {
      Seq.empty[Expression]
    }
  }

  private def getBinaryComparisons(
      viewExpr: Seq[Expression],
      queryAttRef: AttributeReference,
      viewEC: EquivalenceClasses,
      queryTableAliasMap: Map[String, String],
      viewTableAliasMap: Map[String, String])(
      f: (AttributeReference, AttributeReference,
        EquivalenceClasses, Map[String, String],
        Map[String, String]) => Boolean): Seq[BinaryComparison] = {
    var bcs = Seq.empty[BinaryComparison]
    viewExpr.foreach({
      case EqualNullSafe(_, _) =>
        throw new TryMaterializedFailedException(
          "getBinaryComparisons: EqualNullSafe is unsupported")
      case bc@BinaryComparison(ar@AttributeReference(_, _, _, _), Literal(_, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) => bcs :+= bc
      case bc@BinaryComparison(ar@AttributeReference(_, _, _, _), Cast(l@Literal(_, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(ar, l)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          ar@AttributeReference(_, _, _, _), Cast(Cast(l@Literal(_, _), _, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(ar, l)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(Cast(ar@AttributeReference(_, _, _, _), _, _), l@Literal(_, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(ar, l)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(ar@AttributeReference(_, _, _, _), _, _), Cast(l@Literal(_, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(ar, l)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(ar@AttributeReference(_, _, _, _), _, _), Cast(Cast(l@Literal(_, _), _, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(ar, l)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(Cast(ar@AttributeReference(_, _, _, _), _, _), _, _), l@Literal(_, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(ar, l)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(Cast(ar@AttributeReference(_, _, _, _), _, _), _, _), Cast(l@Literal(_, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(ar, l)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(Cast(ar@AttributeReference(_, _, _, _), _, _), _, _),
            Cast(Cast(l@Literal(_, _), _, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(ar, l)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(Literal(_, _), ar@AttributeReference(_, _, _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) => bcs :+= bc
      case bc@BinaryComparison(l@Literal(_, _), Cast(ar@AttributeReference(_, _, _, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(l, ar)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          l@Literal(_, _), Cast(Cast(ar@AttributeReference(_, _, _, _), _, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(l, ar)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(Cast(l@Literal(_, _), _, _), ar@AttributeReference(_, _, _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(l, ar)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(l@Literal(_, _), _, _), Cast(ar@AttributeReference(_, _, _, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(l, ar)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(l@Literal(_, _), _, _), Cast(Cast(ar@AttributeReference(_, _, _, _), _, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(l, ar)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(Cast(l@Literal(_, _), _, _), _, _), ar@AttributeReference(_, _, _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(l, ar)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(Cast(l@Literal(_, _), _, _), _, _), Cast(ar@AttributeReference(_, _, _, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+= bc.makeCopy(Array(l, ar)).asInstanceOf[BinaryComparison]
      case bc@BinaryComparison(
          Cast(Cast(l@Literal(_, _), _, _), _, _),
            Cast(Cast(ar@AttributeReference(_, _, _, _), _, _), _, _))
        if f(queryAttRef, ar, viewEC, queryTableAliasMap, viewTableAliasMap) =>
        bcs :+=bc.makeCopy(Array(l, ar)).asInstanceOf[BinaryComparison]
      case _ =>
    })
    bcs
  }

  private def isAttributeReferenceMatch(
      queryAttRef: AttributeReference,
      viewAttRef: AttributeReference,
      viewEC: EquivalenceClasses,
      queryTableAliasMap: Map[String, String],
      viewTableAliasMap: Map[String, String]): Boolean = {
    val queryAttFullName = getExpressionSql(queryAttRef, queryTableAliasMap)
    val viewAttFullName = getExpressionSql(viewAttRef, viewTableAliasMap)
    queryAttFullName.equals(viewAttFullName) || viewEC.getEquivalenceClassesMap.getOrElse(
      queryAttFullName, Set.empty[String]).contains(viewAttFullName)
  }

  private def mergeColValueScopes(
      attRef: AttributeReference,
      valueScopes: Seq[ColValueScope]): ColValueScope = {
    var merged = valueScopes(0)
    if (valueScopes.size > 1) {
      for (i <- 1 to valueScopes.size - 1) {
        val mcvs = MaterializedViewUtil.mergeColValueScope(attRef.dataType, merged, valueScopes(i))
        if (valueScopes.isEmpty) {
          throw new TryMaterializedFailedException(
            s"mergeColValueScopes: invalid value scope for ${attRef.sql}")
        } else {
          merged = mcvs(0)
        }
      }
    }
    merged
  }

  private def valueScopesToBinaryComparison(
      queryAttRef: AttributeReference,
      valueScopes: Seq[ColValueScope]): Seq[BinaryComparison] = {
    var bcs = Seq.empty[BinaryComparison]
    val dataType = queryAttRef.dataType
    valueScopes.foreach(vs => {
      if (vs.lowValue.isEmpty) {
        if (vs.highValue.isEmpty) {
          throw new TryMaterializedFailedException(
            "valueScopesToBinaryComparison failed: -Max --- Max is not supported")
        } else {
          if (vs.includeHighValue) {
            bcs :+= LessThanOrEqual(queryAttRef,
              MaterializedViewUtil.createLiteralWithType(vs.highValue.get, dataType))
          } else {
            bcs :+= LessThan(queryAttRef,
              MaterializedViewUtil.createLiteralWithType(vs.highValue.get, dataType))
          }
        }
      } else if (vs.highValue.isEmpty) {
        if (vs.includeLowValue) {
          bcs :+= GreaterThanOrEqual(queryAttRef,
            MaterializedViewUtil.createLiteralWithType(vs.lowValue.get, dataType))
        } else {
          bcs :+= GreaterThan(queryAttRef,
            MaterializedViewUtil.createLiteralWithType(vs.lowValue.get, dataType))
        }
      } else {
        if (vs.includeLowValue) {
          bcs :+= GreaterThanOrEqual(queryAttRef,
            MaterializedViewUtil.createLiteralWithType(vs.lowValue.get, dataType))
        } else {
          bcs :+= GreaterThan(queryAttRef,
            MaterializedViewUtil.createLiteralWithType(vs.lowValue.get, dataType))
        }
        if (vs.includeHighValue) {
          bcs :+= LessThanOrEqual(queryAttRef,
            MaterializedViewUtil.createLiteralWithType(vs.highValue.get, dataType))
        } else {
          bcs :+= LessThan(queryAttRef,
            MaterializedViewUtil.createLiteralWithType(vs.highValue.get, dataType))
        }
      }
    })
    bcs
  }
}
