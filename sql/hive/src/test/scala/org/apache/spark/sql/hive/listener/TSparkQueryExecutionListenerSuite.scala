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

package org.apache.spark.sql.hive.listener

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, ResolveSQLOnFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class TSparkQueryExecutionListenerSuite extends SparkFunSuite with SharedSparkSession {

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

  override def beforeAll(): Unit = {
    super.beforeAll()

    catalog.createDatabase(CatalogDatabase("db1", "desc", new URI("loc1"), Map.empty), false)
    val emps = CatalogTable(
      identifier = TableIdentifier("emps", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = empsStruct)
    val depts = CatalogTable(
      identifier = TableIdentifier("depts", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = deptsStruct)
    catalog.createTable(emps, false)
    catalog.createTable(depts, false)

    analyzer = new Analyzer(
      new SessionCatalog(
        catalog,
        FunctionRegistry.builtin,
        new SQLConf().copy(SQLConf.CASE_SENSITIVE -> false)) {
        override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean) {}
      },
      new SQLConf().copy(SQLConf.CASE_SENSITIVE -> false)
    ) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new FindDataSourceTable(spark) +:
          new ResolveSQLOnFile(spark) +: Nil
    }
  }

  test("test1") {
    auditTest("select * from db1.emps", "success", null, Set(createSQLAuditInfo(
      "db1.emps", "read", "success", "Query")))
  }

  test("test2") {
    auditTest("select * from db1.emps", "fail", null, Set(createSQLAuditInfo(
      "db1.emps", "read", "fail", "Query")))
  }

  test("test3") {
    val testSql = "select * from db1.emps, db1.depts where emps.deptno = depts.deptno"
    auditTest(testSql, "success", testSql, Set(
      createSQLAuditInfo("db1.emps", "read", "success", testSql),
      createSQLAuditInfo("db1.depts", "read", "success", testSql)))
  }

  test("test4") {
    val testSql = "select * from db1.emps e, db1.depts d where e.deptno = d.deptno"
    auditTest(testSql, "success", testSql, Set(
      createSQLAuditInfo("db1.emps", "read", "success", testSql),
      createSQLAuditInfo("db1.depts", "read", "success", testSql)))
  }

  test("test5") {
    val testSql = "select * from db1.emps e1, db1.emps e2, db1.depts d" +
      " where e1.deptno = d.deptno and e2.deptno = d.deptno"
    auditTest(testSql, "success", testSql, Set(
      createSQLAuditInfo("db1.emps", "read", "success", testSql),
      createSQLAuditInfo("db1.depts", "read", "success", testSql)))
  }

  test("test6") {
    val testSql = "select * from db1.emps e1" +
      " where e1.deptno in (select deptno from db1.depts)"
    auditTest(testSql, "success", testSql, Set(
      createSQLAuditInfo("db1.emps", "read", "success", testSql),
      createSQLAuditInfo("db1.depts", "read", "success", testSql)))
  }

  test("test7") {
    val testSql = "select * from db1.emps e1, (select deptno from db1.depts) t" +
      " where e1.deptno = t.deptno"
    auditTest(testSql, "success", testSql, Set(
      createSQLAuditInfo("db1.emps", "read", "success", testSql),
      createSQLAuditInfo("db1.depts", "read", "success", testSql)))
  }

  test("test8") {
    val testSql = "select * from db1.emps e1" +
      " where e1.deptno > 1 and e1.deptno in (select deptno from db1.depts)"
    auditTest(testSql, "success", testSql, Set(
      createSQLAuditInfo("db1.emps", "read", "success", testSql),
      createSQLAuditInfo("db1.depts", "read", "success", testSql)))
  }

  test("test9") {
    val testSql = "select * from db1.emps e1" +
      " where e1.deptno > 1 or e1.deptno in (select deptno from db1.depts)"
    auditTest(testSql, "success", testSql, Set(
      createSQLAuditInfo("db1.emps", "read", "success", testSql),
      createSQLAuditInfo("db1.depts", "read", "success", testSql)))
  }

  test("test10") {
    val testSql = "select * from db1.emps e1" +
      " where e1.deptno not in (select deptno from db1.depts)"
    auditTest(testSql, "success", testSql, Set(
      createSQLAuditInfo("db1.emps", "read", "success", testSql),
      createSQLAuditInfo("db1.depts", "read", "success", testSql)))
  }

  test("test11") {
    val testSql = "select * \tfrom \tdb1.emps e1" +
      " where e1.deptno not in (select \tdeptno from db1.depts)"
    auditTest(testSql, "success", testSql, Set(
      createSQLAuditInfo("db1.emps", "read", "success", testSql),
      createSQLAuditInfo("db1.depts", "read", "success", testSql)))
  }

  test("test12") {
    val testSql = "show databases"
    auditTest(testSql, "success", testSql, Set.empty[SQLAuditInfo])
  }

  private def createSQLAuditInfo(
      resource: String,
      action: String,
      status: String,
      comment: String): SQLAuditInfo = {
    val sdfDate = new SimpleDateFormat("yyyyMMdd")
    new SQLAuditInfo("", sdfDate.format(Calendar.getInstance.getTime), "", "", "", "",
      resource, action, status, 1100000000 / 1000000000, comment.replace("\t", " "))
  }

  private def auditTest(
      auditSql: String,
      auditStatus: String,
      auditComment: String,
      expected: Set[SQLAuditInfo]): Unit = {
    try {
      val sc = sparkConf
      sc.set("spark.sql.audit.tdbank.bid", "test_bid")
      sc.set("spark.sql.audit.tdbank.tid", "test_tid")
      val listener = new TSparkQueryExecutionListener(sc)
      val unresolved = parser.parsePlan(auditSql)
      val resolved = analyzer.executeAndCheck(unresolved)
      val sqlAuditInfos = listener.createSQLAuditInfos(
        resolved, auditStatus, 1200000000, auditComment)
      compareAuditInfos(sqlAuditInfos, expected)
    } finally {
    }
  }

  private def compareAuditInfos(actual: Set[SQLAuditInfo], expected: Set[SQLAuditInfo]): Unit = {
    assert(actual.size == expected.size)
    if (actual.isEmpty) return
    val auditBatch = actual.head.auditBatch
    actual.foreach { info =>
      assert(auditBatch == info.auditBatch)
      assert(expected.contains(info))
    }
  }
}
