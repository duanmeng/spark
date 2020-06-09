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

import java.net.URI

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, ResolveSQLOnFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class MaterializedViewOptimizerBaseSuite extends SparkFunSuite with SharedSparkSession {

  val MATERIALIZED_VIEW_DB = "mv_db"
  val catalog = new InMemoryCatalog
  val parser = new SparkSqlParser(new SQLConf)
  val empsStruct = new StructType()
    .add("empid", LongType, nullable = false)
    .add("deptno", IntegerType, nullable = false)
    .add("name", StringType)
    .add("salary", DecimalType(10, 2))
    .add("commission", IntegerType)
  val deptsStruct = new StructType()
    .add("deptno", IntegerType, nullable = false)
    .add("name", StringType)
    .add("location", StringType)
  var analyzer: Analyzer = null
  var sessionCatalog: SessionCatalog = null
  var mvOptimizer: MaterializedViewOptimizer = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    catalog.createDatabase(CatalogDatabase("db1", "desc", new URI("loc1"), Map.empty), false)
    val emps = CatalogTable(
      identifier = TableIdentifier("emps", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = empsStruct)
    catalog.createTable(emps, false)
    catalog.createDatabase(CatalogDatabase("db2", "desc", new URI("loc2"), Map.empty), false)
    val depts = CatalogTable(
      identifier = TableIdentifier("depts", Some("db2")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = deptsStruct)
    catalog.createTable(depts, false)
    val dependents = CatalogTable(
      identifier = TableIdentifier("dependents", Some("db2")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("name", StringType, nullable = false)
        .add("empid", LongType)
        .add("product_id", LongType))
    catalog.createTable(dependents, false)
    val locations = CatalogTable(
      identifier = TableIdentifier("locations", Some("db2")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("name", StringType, nullable = false))
    catalog.createTable(locations, false)


    catalog.createDatabase(CatalogDatabase("foodmart", "desc", new URI("loc3"), Map.empty), false)
    val sf = CatalogTable(
      identifier = TableIdentifier("sales_fact_1997", Some("foodmart")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("time_id", LongType, nullable = false)
        .add("unit_sales", IntegerType)
        .add("product_id", LongType))
    catalog.createTable(sf, false)
    val tbd = CatalogTable(
      identifier = TableIdentifier("time_by_day", Some("foodmart")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("time_id", LongType, nullable = false)
        .add("the_year", IntegerType)
        .add("month_of_year", IntegerType)
        .add("the_month", IntegerType))
    catalog.createTable(tbd, false)
    val p = CatalogTable(
      identifier = TableIdentifier("product", Some("foodmart")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("product_name", StringType, nullable = false)
        .add("product_id", LongType)
        .add("product_class_id", LongType))
    catalog.createTable(p, false)
    val pc = CatalogTable(
      identifier = TableIdentifier("product_class", Some("foodmart")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("product_department", StringType, nullable = false)
        .add("product_category", LongType)
        .add("product_class_id", LongType))
    catalog.createTable(pc, false)

    catalog.createDatabase(CatalogDatabase("mv_db", "desc", new URI("loc3"), Map.empty), false)
    catalog.createDatabase(CatalogDatabase("default", "desc", new URI("loc4"), Map.empty), false)

    sessionCatalog = new SessionCatalog(
      catalog,
      FunctionRegistry.builtin,
      new SQLConf().copy(SQLConf.CASE_SENSITIVE -> false)) {
      override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean) {}
    }

    analyzer = new Analyzer(
      sessionCatalog,
      new SQLConf().copy(SQLConf.CASE_SENSITIVE -> false)
    ) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new FindDataSourceTable(spark) +:
          new ResolveSQLOnFile(spark) +: Nil
    }
    mvOptimizer = new MaterializedViewOptimizer(analyzer)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    doThreadPostAudit()
  }

  protected def withMaterializedView(
      mvName: String,
      mvSchema: StructType,
      mvQuery: String,
      sql: String,
      isPFModelEnabled: Boolean = true)(f: LogicalPlan => Unit): Unit = {
    try {
      analyzer.getSqlConf.setConfString(
        "spark.sql.materializedView.pfmodel.enable", isPFModelEnabled.toString)
      sessionCatalog.createMaterializedView(MATERIALIZED_VIEW_DB, mvName, mvQuery, mvSchema)
      val unresolved = parser.parsePlan(sql)
      val resolved = analyzer.executeAndCheck(unresolved, new QueryPlanningTracker)
      val materialized = mvOptimizer.execute(resolved)
      // scalastyle:off println
      println(getSql(materialized))
      // scalastyle:on println
      assert(materialized.missingInput.isEmpty)
      f(materialized)
    } finally {
      analyzer.getSqlConf.setConfString("spark.sql.materializedView.pfmodel.enable", "true")
      sessionCatalog.dropTable(
        new TableIdentifier(mvName, Some(MATERIALIZED_VIEW_DB)), false, false)
    }
  }

  protected def withMultipleMaterializedView(
      mvs: Seq[TestMaterializedView],
      sql: String,
      isPFModelEnabled: Boolean = true)(f: LogicalPlan => Unit): Unit = {
    try {
      analyzer.getSqlConf.setConfString(
        "spark.sql.materializedView.pfmodel.enable", isPFModelEnabled.toString)
      analyzer.getSqlConf.setConfString(
        "spark.sql.materializedView.matchPolicy", "multiple")
      mvs.foreach {
        tmv => analyzer.getCatalog.createMaterializedView(
          tmv.mvDb, tmv.mvName, tmv.mvQuery, tmv.mvSchema)
      }

      val unresolved = parser.parsePlan(sql)
      val resolved = analyzer.executeAndCheck(unresolved)
      val materialized = mvOptimizer.execute(resolved)
      // scalastyle:off println
      println(getSql(materialized))
      // scalastyle:on println
      assert(materialized.missingInput.isEmpty)
      f(materialized)
    } finally {
      analyzer.getSqlConf.setConfString("spark.sql.materializedView.pfmodel.enable", "true")
      analyzer.getSqlConf.setConfString("spark.sql.materializedView.matchPolicy", "once")
      mvs.foreach {
        tmv => analyzer.getCatalog.dropTable(
          new TableIdentifier(tmv.mvName, Some(tmv.mvDb)), false, false)
      }
    }
  }

  protected def withUnSatisfiedMV(
      mvName: String,
      mvSchema: StructType,
      mvQuery: String,
      sql: String,
      isPFModelEnabled: Boolean = true): Unit = {
    try {
      analyzer.getSqlConf.setConfString(
        "spark.sql.materializedView.pfmodel.enable", isPFModelEnabled.toString)
      sessionCatalog.createMaterializedView(MATERIALIZED_VIEW_DB, mvName, mvQuery, mvSchema)
      val unresolved = parser.parsePlan(sql)
      val resolved = analyzer.executeAndCheck(unresolved, new QueryPlanningTracker)
      val materialized = mvOptimizer.execute(resolved)
      assert(resolved.simpleString(100).equals(materialized.simpleString(100)))
    } finally {
      analyzer.getSqlConf.setConfString("spark.sql.materializedView.pfmodel.enable", "true")
      sessionCatalog.dropTable(
        new TableIdentifier(mvName, Some(MATERIALIZED_VIEW_DB)), false, false)
    }
  }

  protected def getSql(plan: LogicalPlan): String = {
    plan match {
      case Project(_, Sort(orders, _, p@Project(_, _))) =>
        getSqlSingle(p, orders, isOrderAlias = true)
      case Project(_, Sort(orders, _, a@Aggregate(_, _, _))) => getSqlSingle(a, orders)
      case p: Project => getSqlSingle(plan, Seq.empty[SortOrder])
      case a: Aggregate => getSqlSingle(plan, Seq.empty[SortOrder])
      case Sort(orders, _, p@Project(_, _)) => getSqlSingle(p, orders)
      case Sort(orders, _, a@Aggregate(_, _, _)) => getSqlSingle(a, orders)
      case Union(children) =>
        val sb = StringBuilder.newBuilder
        for (i <- 0 to children.size - 1) {
          sb.append(getSql(children(i)))
          if (i < children.size - 1) {
            sb.append("union all")
          }
        }
        sb.toString
      case d@Distinct(p@Project(_, _)) => getSqlSingle(p, Seq.empty[SortOrder], isDistinct = true)
    }
  }

  protected def getSqlSingle(
      plan: LogicalPlan,
      orders: Seq[SortOrder],
      isDistinct: Boolean = false,
      isOrderAlias: Boolean = false): String = {
    var outputs = Seq.empty[String]
    var tables = Seq.empty[String]
    var parsedTables = Set.empty[String]
    var groupBys = Seq.empty[String]
    var filter = ""
    var subquerys = Seq.empty[String]
    var joinConditions = Seq.empty[String]
    var inSubquery = false

    plan match {
      case Project(pl, _) =>
        pl.foreach(
          exp =>
            exp match {
              case ar: AttributeReference =>
                if (!isOrderAlias) {
                  outputs :+= exp.sql
                }
              case _ => outputs :+= exp.sql
            }
        )
      case Aggregate(ges, aes, _) =>
        aes.foreach(
          exp => outputs :+= exp.sql
        )
        ges.foreach(
          exp => groupBys :+= exp.sql
        )
    }

    plan.foreach {
      case SubqueryAlias(name, a@Project(_, _)) =>
        if (!name.equals("__auto_generated_subquery_name")) {
          subquerys :+= "(" + getSql(a) + ") as " + name
        } else {
          subquerys :+= "(" + getSql(a) + ")"
        }
        inSubquery = true
      case SubqueryAlias(name, a@Aggregate(_, _, _)) =>
        if (!name.equals("__auto_generated_subquery_name")) {
          subquerys :+= "(" + getSql(a) + ") as " + name
        } else {
          subquerys :+= "(" + getSql(a) + ")"
        }
        inSubquery = true
      case SubqueryAlias(aliasName, SubqueryAlias(tableName, _)) =>
        if (!inSubquery) {
          parsedTables += tableName.toString
          tables :+= tableName.toString + " as " + aliasName.toString
        }
      case SubqueryAlias(tableName, child)
        if (!child.isInstanceOf[Project] && !child.isInstanceOf[Aggregate]) =>
        if (!parsedTables.contains(tableName.toString) && !inSubquery) {
          parsedTables += tableName.toString
          tables :+= tableName.toString
        }
      case Join(_, _, jt@Inner, condition, _) =>
        if (condition.isDefined) {
          joinConditions :+= condition.get.sql
        }
      case _ =>
    }

    // extract filter, avoid confuse with subquery's
    plan match {
      case Project(_, Filter(condition, _)) => filter = condition.sql
      case Aggregate(_, _, Filter(condition, _)) => filter = condition.sql
      case _ =>
    }

    var orderStrs = Seq.empty[String]
    orders.foreach(
      order => orderStrs :+= order.sql
    )

    val sb = StringBuilder.newBuilder
    if (isDistinct) {
      sb.append("\nselect distinct ").append(outputs.sorted.mkString(", "))
    } else {
      sb.append("\nselect ").append(outputs.sorted.mkString(", "))
    }
    sb.append("\nfrom ").append(tables.sorted.mkString(", "))
    if (!subquerys.isEmpty) {
      if (!tables.isEmpty) {
        sb.append(", ")
      }
      sb.append(subquerys.sorted.mkString(", "))
    }
    if (!joinConditions.isEmpty) {
      sb.append("\nwhere ").append(joinConditions.sorted.mkString(" and "))
      if (!filter.isEmpty) {
        sb.append(" and ").append(filter)
      }
    } else if (!filter.isEmpty) {
      sb.append("\nwhere ").append(filter)
    }

    if (!groupBys.isEmpty) {
      sb.append("\ngroup by ").append(groupBys.sorted.mkString(", "))
    }

    if (!orderStrs.isEmpty) {
      sb.append("\norder by ").append(orderStrs.sorted.mkString(", "))
    }

    sb.append("\n")
    sb.toString
  }
}

class TestMaterializedView(
    val mvDb: String,
    val mvName: String,
    val mvSchema: StructType,
    val mvQuery: String)
