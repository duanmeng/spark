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

import org.apache.spark.sql.types._

class MaterializedViewOptimizerSub1Suite extends MaterializedViewOptimizerBaseSuite {

  // scalastyle:off
  test("test function in output") {
    withMaterializedView("testmv",
        mvSchema = empsStruct,
        mvQuery = "select * from db1.emps where deptno = 10",
        sql = "select empid + 1 as empAdd1, empid as newEmpId from db1.emps where deptno = 10") {
      materialized =>

        assert(
        """
          |select (mv_db.testmv.`empid` + CAST(1 AS BIGINT)) AS `empAdd1`, mv_db.testmv.`empid` AS `newEmpId`
          |from mv_db.testmv
          |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilterToProject1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType)
        .add("s", DecimalType(10, 2)),
      mvQuery =
        """
          |select deptno, count(*) as c, sum(salary) as s
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select *
          |from (select deptno, count(*) as c, sum(salary) as s from db1.emps group by deptno)
          |where (s * 0.8) > 10000
          |""".stripMargin) {
      materialized =>
        assert(
          """
            |select __auto_generated_subquery_name.`c`, __auto_generated_subquery_name.`deptno`, __auto_generated_subquery_name.`s`
            |from (
            |select mv_db.testmv.`c` AS `c`, mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`s` AS `s`
            |from mv_db.testmv
            |) as `__auto_generated_subquery_name`
            |where (CAST((CAST(__auto_generated_subquery_name.`s` AS DECIMAL(20,2)) * CAST(0.8BD AS DECIMAL(20,2))) AS DECIMAL(22,3)) > CAST(CAST(10000 AS DECIMAL(5,0)) AS DECIMAL(22,3)))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilterQueryOnProjectView0") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery = "select deptno, empid from db1.emps",
      sql = "select empid + 1 as x from db1.emps where deptno = 10") {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`empid` + CAST(1 AS BIGINT)) AS `x`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 10)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnProjectView0 but with extra column in
   * materialized view. */
  test("testFilterQueryOnProjectView1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery = "select deptno, empid, name from db1.emps",
      sql = "select empid + 1 from db1.emps where deptno = 10") {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`empid` + CAST(1 AS BIGINT)) AS `(empid + CAST(1 AS BIGINT))`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 10)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnProjectView0 but with extra column in both
   * materialized view and query. */
  test("testFilterQueryOnProjectView2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery = "select deptno, empid, name from db1.emps",
      sql = "select empid + 1, name from db1.emps where deptno = 10") {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`empid` + CAST(1 AS BIGINT)) AS `(empid + CAST(1 AS BIGINT))`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 10)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilterQueryOnProjectView3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empAdd1", LongType, nullable = false)
        .add("x", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery = "select deptno - 10 as x, empid + 1 as empAdd1, name from db1.emps",
      sql = "select name from db1.emps where deptno - 10 = 0") {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`x` = 0)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnProjectView3 but materialized view cannot
   * be used because it does not contain required expression. */
  test("testFilterQueryOnProjectView4") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("empAdd1", LongType, nullable = false)
        .add("x", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery = "select deptno - 10 as x, empid + 1 as empAdd1, name from db1.emps",
      sql = "select name from db1.emps where deptno + 10 = 20")
  }

  /** As testFilterQueryOnProjectView3 but also contains an
   * expression column. */
  test("testFilterQueryOnProjectView5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empAdd1", LongType, nullable = false)
        .add("x", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery = "select deptno - 10 as x, empid + 1 as empAdd1, name from db1.emps",
      sql = "select name, empid + 1 as e from db1.emps where deptno - 10 = 2") {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empAdd1` AS `e`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`x` = 2)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** Cannot materialize because "name" is not projected in the MV. */
  test("testFilterQueryOnProjectView6") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("x", IntegerType, nullable = false),
      mvQuery = "select deptno - 10 as x, empid from db1.emps",
      sql = "select name from db1.emps where deptno - 10 = 0")
  }

  /** As testFilterQueryOnProjectView6 but also contains an
   * expression column. */
  test("testFilterQueryOnProjectView7") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("empAdd1", LongType, nullable = false)
        .add("x", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery = "select deptno - 10 as x, empid + 1 as empAdd1, name from db1.emps",
      sql = "select name, empid + 2 from db1.emps where deptno - 10 = 0")
  }

  test("testFilterQueryOnFilterView") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery = "select deptno, empid, name from db1.emps where deptno = 10",
      sql = "select name, empid + 1 as x from db1.emps where deptno = 10") {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`empid` + CAST(1 AS BIGINT)) AS `x`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnFilterView but condition is stronger in
   * query. */
  test("testFilterQueryOnFilterView2") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery = "select deptno, empid, name from db1.emps where deptno = 10",
      sql = "select name, empid + 1 as x from db1.emps where deptno = 10 and empid < 150") {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`empid` + CAST(1 AS BIGINT)) AS `x`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` < CAST(150 AS BIGINT))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnFilterView but condition is weaker in
   * view. */
  test("testFilterQueryOnFilterView3") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select deptno, empid, name
          |from db1.emps
          |where deptno = 10 or deptno = 20 or empid < 160
          |""".stripMargin,
      sql =
        """
          |select name, empid + 1 as x
          |from db1.emps
          |where deptno = 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`empid` + CAST(1 AS BIGINT)) AS `x`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 10)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnFilterView but condition is stronger in
   * query. */
  test("testFilterQueryOnFilterView4") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select *
          |from db1.emps
          |where deptno > 10
          |""".stripMargin,
      sql =
        """
          |select name
          |from db1.emps
          |where deptno > 30
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 30)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnFilterView but condition is stronger in
   * query. */
  test("testFilterQueryOnFilterView5") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select name, deptno
          |from db1.emps
          |where deptno > 10
          |""".stripMargin,
      sql =
        """
          |select name
          |from db1.emps
          |where deptno > 30
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 30)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnFilterView but condition is stronger in
   * query and columns selected are subset of columns in materialized view. */
  test("testFilterQueryOnFilterView6") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select name, deptno, salary
          |from db1.emps
          |where salary > 2000.5
          |""".stripMargin,
      sql =
        """
          |select name
          |from db1.emps
          |where deptno > 30 and salary > 3000
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where ((mv_db.testmv.`deptno` > 30) AND (CAST(mv_db.testmv.`salary` AS DECIMAL(10,2)) > CAST(CAST(3000 AS DECIMAL(4,0)) AS DECIMAL(10,2))))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnFilterView but condition is stronger in
   * query and columns selected are subset of columns in materialized view.
   * Condition here is complex. */
  test("testFilterQueryOnFilterView7") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select *
          |from db1.emps
          |where (salary < 1111.9 and deptno > 10)
          |   or (empid > 400 and salary > 5000)
          |   or salary > 500
          |""".stripMargin,
      sql =
        """
          |select name
          |from db1.emps
          |where salary > 1000
          |   or (deptno >= 30 and salary <= 500)
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where ((CAST(mv_db.testmv.`salary` AS DECIMAL(10,2)) > CAST(CAST(1000 AS DECIMAL(4,0)) AS DECIMAL(10,2))) OR ((mv_db.testmv.`deptno` >= 30) AND (CAST(mv_db.testmv.`salary` AS DECIMAL(10,2)) <= CAST(CAST(500 AS DECIMAL(3,0)) AS DECIMAL(10,2)))))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** As testFilterQueryOnFilterView but condition is stronger in
   * query. However, columns selected are not present in columns of materialized
   * view, Hence should not use materialized view. */
  test("testFilterQueryOnFilterView8") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery = "select deptno, name from db1.emps where deptno > 10",
      sql = "select name, empid from db1.emps where deptno > 30")
  }

  /** As testFilterQueryOnFilterView but condition is weaker in
   * query. */
  test("testFilterQueryOnFilterView9") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery = "select deptno, name from db1.emps where deptno > 10",
      sql = "select name, empid from db1.emps where deptno > 30 or empid > 10")
  }

  /** As testFilterQueryOnFilterView but condition currently
   * has unsupported type being checked on query. */
  test("testFilterQueryOnFilterView10") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery = "select deptno, name from db1.emps where deptno > 10 and name = 'sparksql'",
      sql = "select name from db1.emps where deptno > 30")
  }

  /** As testFilterQueryOnFilterView but condition is weaker in
   * query and columns selected are subset of columns in materialized view.
   * Condition here is complex. */
  test("testFilterQueryOnFilterView11") {
    withUnSatisfiedMV("testmv",
        mvSchema = new StructType()
          .add("deptno", IntegerType, nullable = false)
          .add("name", StringType),
        mvQuery =
          """
            |select deptno, name
            |from db1.emps
            |where (salary < 1111.9 and deptno > 10)
            |   or (empid > 400 and salary > 5000)
            |""".stripMargin,
        sql = "select name from db1.emps where deptno > 30 and salary > 3000")
  }

  /** As testFilterQueryOnFilterView but condition of
   * query is stronger but is on the column not present in MV (salary).
   */
  test("testFilterQueryOnFilterView12") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery =
        """
          |select deptno, name
          |from db1.emps
          |where salary > 2000.5
          |""".stripMargin,
      sql = "select name from db1.emps where deptno > 30 and salary > 3000")
  }

  /** As testFilterQueryOnFilterView but condition is weaker in
   * query and columns selected are subset of columns in materialized view.
   * Condition here is complex. */
  test("testFilterQueryOnFilterView13") {
    withUnSatisfiedMV("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select *
          |from db1.emps
          |where (salary > 1111.9 and deptno > 10)
          |  or (empid > 400 and salary > 5000)
          |""".stripMargin,
      sql =
        """
          |select name
          |from db1.emps
          |where salary > 1000
          |  or (deptno > 30 and salary > 3000)
          |""".stripMargin)
  }

  /** As testFilterQueryOnFilterView7 but columns in materialized
   * view are a permutation of columns in the query. */
  test("testFilterQueryOnFilterView14") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select deptno, empid, name, salary, commission
          |from db1.emps as em
          |where (salary < 1111.9 and deptno > 10)
          |  or (empid > 400 and salary > 5000)
          |  or salary > 500
          |""".stripMargin,
      sql =
        """
          |select *
          |from db1.emps
          |where salary > 1000
          |  or (deptno >= 30 and salary <= 500)
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`commission` AS `commission`, mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`name` AS `name`, mv_db.testmv.`salary` AS `salary`
            |from mv_db.testmv
            |where ((CAST(mv_db.testmv.`salary` AS DECIMAL(10,2)) > CAST(CAST(1000 AS DECIMAL(4,0)) AS DECIMAL(10,2))) OR ((mv_db.testmv.`deptno` >= 30) AND (CAST(mv_db.testmv.`salary` AS DECIMAL(10,2)) <= CAST(CAST(500 AS DECIMAL(3,0)) AS DECIMAL(10,2)))))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** Aggregation query at same level of aggregation as aggregation
   * materialization. */
  test("testAggregate0") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("c", LongType, nullable = false),
      mvQuery = "select count(*) as c from db1.emps group by empid",
      sql = "select count(*) + 1 as c from db1.emps group by empid") {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`c` + CAST(1 AS BIGINT)) AS `c`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /**
   * Aggregation query at same level of aggregation as aggregation
   * materialization but with different row types. */
  test("testAggregate1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("c0", LongType, nullable = false),
      mvQuery = "select count(*) as c0 from db1.emps group by empid",
      sql = "select count(*) as c1 from db1.emps group by empid") {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c0` AS `c1`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregate2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false),
      mvQuery = "select deptno, count(*) as c, sum(empid) as s from db1.emps group by deptno",
      sql = "select count(*) + 1 as c, deptno from db1.emps group by deptno") {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`c` + CAST(1 AS BIGINT)) AS `c`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregate3") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select empid, deptno, name, salary, commission
          |from db1.emps
          |group by empid, deptno, name, salary, commission
          |""".stripMargin,
      sql =
        """
          |select deptno, sum(salary), sum(k)
          |from (
          |  select deptno, salary, 100 as k
          |  from db1.emps
          |  group by empid, deptno, name, salary, commission
          |)
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select __auto_generated_subquery_name.`deptno`, sum(CAST(__auto_generated_subquery_name.`k` AS BIGINT)) AS `sum(k)`, sum(__auto_generated_subquery_name.`salary`) AS `sum(salary)`
            |from (
            |select 100 AS `k`, mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`salary` AS `salary`
            |from mv_db.testmv
            |) as `__auto_generated_subquery_name`
            |group by __auto_generated_subquery_name.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregate4") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("commission", IntegerType, nullable = false)
        .add("s", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, commission, sum(salary) as s
          |from db1.emps
          |group by deptno, commission
          |""".stripMargin,
      sql =
        """
          |select deptno, sum(salary)
          |from db1.emps
          |where commission = 100
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, sum(mv_db.testmv.`s`) AS `sum(salary)`
            |from mv_db.testmv
            |where (mv_db.testmv.`commission` = 100)
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregate5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("dc", LongType, nullable = false)
        .add("commission", LongType, nullable = false)
        .add("s", LongType, nullable = false),
      mvQuery =
        """
          |select deptno + commission as dc, commission, sum(salary) as s
          |from db1.emps
          |group by deptno + commission, commission
          |""".stripMargin,
      sql =
        """
          |select commission, sum(salary)
          |from db1.emps
          |where commission * (deptno + commission) = 100
          |group by commission
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`commission` AS `commission`, sum(mv_db.testmv.`s`) AS `sum(salary)`
            |from mv_db.testmv
            |where ((mv_db.testmv.`commission` * mv_db.testmv.`dc`) = CAST(100 AS BIGINT))
            |group by mv_db.testmv.`commission`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /**
   * Matching failed because the filtering condition under Aggregate
   * references columns for aggregation.
   */
  test("testAggregate6") {
      withUnSatisfiedMV("testmv",
        mvSchema = new StructType()
          .add("deptno", IntegerType, nullable = false)
          .add("sum_salary", LongType, nullable = false)
          .add("sc", LongType, nullable = false),
        mvQuery =
          """
            |select deptno, sum(salary) as sum_salary, sum(commission) as sc
            |from db1.emps
            |group by deptno
            |""".stripMargin,
        sql =
          """
            |select * from
            |(select deptno, sum(salary) as sum_salary
            |from db1.emps
            |where salary > 1000
            |group by deptno)
            |where sum_salary > 10
            |""".stripMargin)
  }

  /**
   * There will be a compensating Project added after matching of the Aggregate.
   * This rule targets to test if the SparkSQL can be handled.
   */
  test("testCompensatingCalcWithAggregate0") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("sum_salary", LongType, nullable = false)
        .add("sc", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, sum(salary) as sum_salary, sum(commission) as sc
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select * from
          |(select deptno, sum(salary) as sum_salary
          |from db1.emps
          |group by deptno)
          |where sum_salary > 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select __auto_generated_subquery_name.`deptno`, __auto_generated_subquery_name.`sum_salary`
            |from (
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`sum_salary` AS `sum_salary`
            |from mv_db.testmv
            |) as `__auto_generated_subquery_name`
            |where (CAST(__auto_generated_subquery_name.`sum_salary` AS DECIMAL(20,2)) > CAST(CAST(10 AS DECIMAL(2,0)) AS DECIMAL(20,2)))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /**
   * There will be a compensating Project + Filter added after matching of the Aggregate.
   * This rule targets to test if the Calc can be handled.
   */
  test("testCompensatingCalcWithAggregate1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("sum_salary", LongType, nullable = false)
        .add("sc", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, sum(salary) as sum_salary, sum(commission) as sc
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select * from
          |(select deptno, sum(salary) as sum_salary
          |from db1.emps
          |where deptno >= 20
          |group by deptno)
          |where sum_salary > 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select __auto_generated_subquery_name.`deptno`, __auto_generated_subquery_name.`sum_salary`
            |from (
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`sum_salary` AS `sum_salary`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` >= 20)
            |) as `__auto_generated_subquery_name`
            |where (CAST(__auto_generated_subquery_name.`sum_salary` AS DECIMAL(20,2)) > CAST(CAST(10 AS DECIMAL(2,0)) AS DECIMAL(20,2)))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /**
   * There will be a compensating Project + Filter added after matching of the Aggregate.
   * This rule targets to test if the Calc can be handled.
   */
  test("testCompensatingCalcWithAggregate2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("sum_salary", LongType, nullable = false)
        .add("sc", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, sum(salary) as sum_salary, sum(commission) as sc
          |from db1.emps
          |where deptno >= 10
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select * from
          |(select deptno, sum(salary) as sum_salary
          |from db1.emps
          |where deptno >= 20
          |group by deptno)
          |where sum_salary > 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select __auto_generated_subquery_name.`deptno`, __auto_generated_subquery_name.`sum_salary`
            |from (
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`sum_salary` AS `sum_salary`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` >= 20)
            |) as `__auto_generated_subquery_name`
            |where (CAST(__auto_generated_subquery_name.`sum_salary` AS DECIMAL(20,2)) > CAST(CAST(10 AS DECIMAL(2,0)) AS DECIMAL(20,2)))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** Aggregation materialization with a project. */
  test("testAggregateProject") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("emp2", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, count(*) as c, empid + 2 as emp2, sum(empid) as s
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select count(*) + 1 as c, deptno
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>
        assert(
          """
            |select (sum(mv_db.testmv.`c`) + CAST(1 AS BIGINT)) AS `c`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateOnProject1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("empid", IntegerType, nullable = false),
      mvQuery =
        """
          |select empid, deptno, count(*) as c, sum(empid) as s
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select count(*) + 1 as c, deptno
          |from db1.emps
          |group by deptno, empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`c` + CAST(1 AS BIGINT)) AS `c`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateOnProject5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType)
        .add("c", LongType, nullable = false)
        .add("empid", IntegerType, nullable = false),
      mvQuery =
        """
          |select empid, deptno, name, count(*) as c
          |from db1.emps
          |group by empid, deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, empid, count(*)
          |from db1.emps
          |group by name, empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`name` AS `name`, sum(mv_db.testmv.`c`) AS `count(1)`
            |from mv_db.testmv
            |group by mv_db.testmv.`empid`, mv_db.testmv.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateOnProjectAndFilter") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, sum(salary) as s, count(1) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, count(1)
          |from db1.emps
          |where deptno = 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `count(1)`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 10)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testProjectOnProject") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("s2", LongType, nullable = false)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, sum(salary) + 2 as s2, sum(commission) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, sum(salary) + 2
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`s2` AS `(CAST(sum(salary) AS DECIMAL(21,2)) + CAST(CAST(2 AS DECIMAL(1,0)) AS DECIMAL(21,2)))`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testPermutationError") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", IntegerType, nullable = false)
        .add("mins", LongType, nullable = false)
        .add("maxs", LongType, nullable = false)
        .add("ss", LongType, nullable = false)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select min(salary) as mins, count(*) as c, max(salary) as maxs, sum(salary) as ss, empid
          |from db1.emps
          |group by empid
          |""".stripMargin,
      sql =
        """
          |select count(*), empid
          |from db1.emps
          |group by empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `count(1)`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinOnLeftProjectToJoin") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("esums", LongType, nullable = false)
        .add("esumc", LongType, nullable = false)
        .add("dc", LongType, nullable = false),
      mvQuery =
        """
          |select e.deptno, sum(e.salary) as esums, sum(e.commission) as esumc, count(d.name) as dc
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |group by e.deptno
          |""".stripMargin,
      sql =
        """
          |select e.deptno, sum(e.salary) as esums1, count(d.name)
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |group by e.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`dc` AS `count(name)`, mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`esums` AS `esums1`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinOnRightProjectToJoin") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("esums", LongType, nullable = false)
        .add("esumc", LongType, nullable = false)
        .add("dc", LongType, nullable = false),
      mvQuery =
        """
          |select e.deptno, sum(e.salary) as esums, sum(e.commission) as esumc, count(d.name) as dc
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |group by e.deptno
          |""".stripMargin,
      sql =
        """
          |select e.deptno, sum(e.salary) as esums1, sum(e.commission) as esumc1, count(d.name) as dc1
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |group by e.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`dc` AS `dc1`, mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`esumc` AS `esumc1`, mv_db.testmv.`esums` AS `esums1`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinOnCalcToJoin0") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, depts.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select e.deptno as d1, e.empid, d.deptno as d2
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |where e.deptno > 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `d1`, mv_db.testmv.`deptno` AS `d2`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 10)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinOnCalcToJoin1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno1", IntegerType, nullable = false)
        .add("deptno2", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, depts.deptno as deptno2, emps.deptno as deptno1
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select e.deptno as d1, e.empid, d.deptno as d2
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |where e.deptno > 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno1` AS `d1`, mv_db.testmv.`deptno2` AS `d2`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno1` > 10)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinOnCalcToJoin2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno1", IntegerType, nullable = false)
        .add("deptno2", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, depts.deptno as deptno2, emps.deptno as deptno1
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select e.deptno as d1, e.empid, d.deptno as d2
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |where d.deptno > 10
          |  and e.empid > 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno1` AS `d1`, mv_db.testmv.`deptno2` AS `d2`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where ((mv_db.testmv.`deptno2` > 10) AND (mv_db.testmv.`empid` > CAST(10 AS BIGINT)))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  // check if columns equals expression match,
  // eg, t1.c1 = t2.c1 should be in both query and view
  test("testJoinOnCalcToJoin3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno1", IntegerType, nullable = false)
        .add("deptno2", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, depts.deptno as deptno2, emps.deptno as deptno1
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select e.deptno as d1, e.empid, d.deptno as d2
          |from db1.emps e join db2.depts d on e.deptno = d.deptno + 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select d.`deptno` AS `d2`, e.`deptno` AS `d1`, e.`empid`
            |from db1.emps as e, db2.depts as d
            |where (e.`deptno` = (d.`deptno` + 1))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testSwapJoin") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("cv", LongType, nullable = false),
      mvQuery =
        """
          |select count(*) as cv
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select count(*) as cq
          |from db2.depts d join db1.emps e on d.deptno = e.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`cv` AS `cq`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** Tests a complicated star-join query on a complicated materialized
   * star-join query. Some of the features:
   *
   * <ol>
   * <li>query joins in different order;
   * <li>query's join conditions are in where clause;
   * <li>query does not use all join tables (safe to omit them because they are
   * many-to-mandatory-one joins);
   * <li>query is at higher granularity, therefore needs to roll up;
   * <li>query has a condition on one of the materialization's grouping columns.
   * </ol>
   */
  test("testFilterGroupQueryOnStar") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("c", LongType, nullable = false)
        .add("sum_unit_sales", LongType, nullable = false)
        .add("the_year", IntegerType, nullable = false)
        .add("the_month", IntegerType, nullable = false)
        .add("product_name", StringType, nullable = false),
      mvQuery =
        """
          |select p.product_name, t.the_year, t.the_month, sum(f.unit_sales) as sum_unit_sales, count(*) as c
          |from foodmart.sales_fact_1997 as f
          |join foodmart.time_by_day as t on f.time_id = t.time_id
          |join foodmart.product as p on f.product_id = p.product_id
          |join foodmart.product_class as pc on p.product_class_id = pc.product_class_id
          |group by t.the_year, t.the_month, pc.product_department, pc.product_category, p.product_name
          |""".stripMargin,
      sql =
        """
          |select t.the_month, count(*) as x
          |from foodmart.time_by_day as t, foodmart.sales_fact_1997 as f
          |where t.the_year = 1997
          |  and t.time_id = f.time_id
          |group by t.the_year, t.the_month
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`the_month` AS `the_month`, sum(mv_db.testmv.`c`) AS `x`
            |from mv_db.testmv
            |where (mv_db.testmv.`the_year` = 1997)
            |group by mv_db.testmv.`the_month`, mv_db.testmv.`the_year`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  /** Simpler than {@link #testFilterGroupQueryOnStar()}, tests a query on a
   * materialization that is just a join. */
  test("testQueryOnStar") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("product_name", StringType, nullable = false)
        .add("the_year", IntegerType, nullable = false)
        .add("month_of_year", IntegerType, nullable = false)
        .add("unit_sales", LongType, nullable = false)
        .add("product_department", StringType, nullable = false),
      mvQuery =
        """
          |select p.product_name, t.the_year, f.unit_sales, pc.product_department, t.month_of_year
          |from foodmart.sales_fact_1997 as f
          |join foodmart.time_by_day as t on f.time_id = t.time_id
          |join foodmart.product as p on f.product_id = p.product_id
          |join foodmart.product_class as pc on p.product_class_id = pc.product_class_id
          |""".stripMargin,
      sql =
        """
          |select p.product_name, t.the_year, f.unit_sales, pc.product_department
          |from foodmart.sales_fact_1997 as f
          |join foodmart.time_by_day as t on f.time_id = t.time_id
          |join foodmart.product as p on f.product_id = p.product_id
          |join foodmart.product_class as pc on p.product_class_id = pc.product_class_id
          |where t.month_of_year = 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`product_department` AS `product_department`, mv_db.testmv.`product_name` AS `product_name`, mv_db.testmv.`the_year` AS `the_year`, mv_db.testmv.`unit_sales` AS `unit_sales`
            |from mv_db.testmv
            |where (mv_db.testmv.`month_of_year` = 10)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterialization") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery =
        """
          |select emps.empid, depts.deptno, emps.name
          |from db1.emps join db2.depts depts on emps.deptno = depts.deptno
          |where emps.empid < 500
          |""".stripMargin,
      sql =
        """
          |select empid, name
          |from db1.emps
          |where empid < 300
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` < CAST(300 AS BIGINT))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  // join condition in where
  test("testJoinMaterialization1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery =
        """
          |select emps.empid, depts.deptno, emps.name
          |from db1.emps join db2.depts depts
          |where emps.empid < 500
          |  and emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select empid, name
          |from db1.emps
          |where empid < 300
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` < CAST(300 AS BIGINT))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterialization2") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select deptno, empid, name, salary, commission
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select empid, emps.name, depts.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db2.depts.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`name` AS `name`
            |from db2.depts, mv_db.testmv
            |where (mv_db.testmv.`deptno` = db2.depts.`deptno`)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterialization3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select empid, depts.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select empid, depts.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where empid = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` = CAST(1 AS BIGINT))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testUnionAll") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select * from db1.emps where empid < 500
          |""".stripMargin,
      sql =
        """
          |select * from db1.emps where empid > 300
          |union all
          |select * from db1.emps where empid < 200
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db1.emps.`commission`, db1.emps.`deptno`, db1.emps.`empid`, db1.emps.`name`, db1.emps.`salary`
            |from db1.emps
            |where (db1.emps.`empid` > CAST(300 AS BIGINT))
            |union all
            |select mv_db.testmv.`commission` AS `commission`, mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`name` AS `name`, mv_db.testmv.`salary` AS `salary`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` < CAST(200 AS BIGINT))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationNoAggregateFuncs1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select empid, deptno
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select empid, deptno
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationNoAggregateFuncs2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select empid, deptno
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationNoAggregateFuncs3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select empid, deptno
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db1.emps.`deptno`, db1.emps.`empid`
            |from db1.emps
            |group by db1.emps.`deptno`, db1.emps.`empid`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationNoAggregateFuncs4") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select empid, deptno
          |from db1.emps
          |where deptno = 10
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno = 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationNoAggregateFuncs5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select empid, deptno
          |from db1.emps
          |where deptno = 5
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno = 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db1.emps.`deptno`
            |from db1.emps
            |where (db1.emps.`deptno` = 10)
            |group by db1.emps.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationNoAggregateFuncs6") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select empid, deptno
          |from db1.emps
          |where deptno > 5
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno > 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 10)
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationNoAggregateFuncs7") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select empid, deptno
          |from db1.emps
          |where deptno > 5
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno < 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db1.emps.`deptno`
            |from db1.emps
            |where (db1.emps.`deptno` < 10)
            |group by db1.emps.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationNoAggregateFuncs8") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db1.emps.`deptno`
            |from db1.emps
            |group by db1.emps.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationNoAggregateFuncs9") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, deptno
          |from db1.emps
          |where salary > 1000
          |group by name, empid, deptno
          |""".stripMargin,
      sql =
        """
          |select empid
          |from db1.emps
          |where salary > 2000
          |group by name, empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db1.emps.`empid`
            |from db1.emps
            |where (CAST(db1.emps.`salary` AS DECIMAL(10,2)) > CAST(CAST(2000 AS DECIMAL(4,0)) AS DECIMAL(10,2)))
            |group by db1.emps.`empid`, db1.emps.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationAggregateFuncs1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, deptno, count(*) as c, sum(empid) as s
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationAggregateFuncs2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, deptno, count(*) as c, sum(empid) as s
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, count(*) as c, sum(empid) as s
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, sum(mv_db.testmv.`c`) AS `c`, sum(mv_db.testmv.`s`) AS `s`
            |from mv_db.testmv
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationAggregateFuncs3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, deptno, count(*) as c, sum(empid) as s
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, empid, count(*) as c, sum(empid) as s
          |from db1.emps
          |group by deptno, empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c`, mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`s` AS `s`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationAggregateFuncs4") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, deptno, count(*) as c, sum(empid) as s
          |from db1.emps
          |where deptno >= 10
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, sum(empid) as s
          |from db1.emps
          |where deptno > 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, sum(mv_db.testmv.`s`) AS `s`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 10)
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationAggregateFuncs5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, deptno, count(*) + 1 as c, sum(empid) as s
          |from db1.emps
          |where deptno >= 10
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, sum(empid) + 1 as s
          |from db1.emps
          |where deptno > 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select (sum(mv_db.testmv.`s`) + CAST(1 AS BIGINT)) AS `s`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 10)
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  // better than calcite for the origin case, but has bug with "sum(empid) + 2 as s2" in query
  // it should be record as a known bug
  test("testAggregateMaterializationAggregateFuncs6") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, deptno, count(*) + 1 as c, sum(empid) + 2 as s
          |from db1.emps
          |where deptno >= 10
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, sum(empid) + 1 as s1
          |from db1.emps
          |where deptno > 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select (sum(mv_db.testmv.`empid`) + CAST(1 AS BIGINT)) AS `s1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 10)
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationAggregateFuncs7") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, deptno, count(*) + 1 as c, sum(empid) as s
          |from db1.emps
          |where deptno >= 10
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno + 1, sum(empid) + 1 as s1
          |from db1.emps
          |where deptno > 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select (mv_db.testmv.`deptno` + 1) AS `(deptno + 1)`, (sum(mv_db.testmv.`s`) + CAST(1 AS BIGINT)) AS `s1`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 10)
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationAggregateFuncs8") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("d1", IntegerType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, deptno + 1 as d1, count(*) + 1 as c, sum(empid) as s
          |from db1.emps
          |where deptno >= 10
          |group by empid, deptno
          |""".stripMargin,
      sql =
        """
          |select deptno + 1, sum(empid) + 1 as s
          |from db1.emps
          |where deptno > 10
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select (db1.emps.`deptno` + 1) AS `(deptno + 1)`, (sum(db1.emps.`empid`) + CAST(1 AS BIGINT)) AS `s`
            |from db1.emps
            |where (db1.emps.`deptno` > 10)
            |group by db1.emps.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationNoAggregateFuncs1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, depts.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where depts.deptno > 10
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select empid
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where depts.deptno > 20
          |group by empid, depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 20)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationNoAggregateFuncs2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select depts.deptno, empid
          |from db2.depts join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 10
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select empid
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where depts.deptno > 20
          |group by empid, depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 20)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationNoAggregateFuncs3") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid
          |from db2.depts join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 10
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select empid
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where depts.deptno > 20
          |group by empid, depts.deptno
          |""".stripMargin)
  }

  test("testJoinAggregateMaterializationNoAggregateFuncs5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select depts.deptno, empid
          |from db2.depts join db1.emps on emps.deptno = depts.deptno
          |where emps.empid > 10
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts join db1.emps on emps.deptno = depts.deptno
          |where emps.empid > 15
          |group by depts.deptno, emps.empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` > CAST(15 AS BIGINT))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationNoAggregateFuncs6") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select depts.deptno, empid
          |from db2.depts join db1.emps on emps.deptno = depts.deptno
          |where emps.empid > 10
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts join db1.emps on emps.deptno = depts.deptno
          |where emps.empid > 15
          |group by depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` > CAST(15 AS BIGINT))
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationNoAggregateFuncs7") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select depts.deptno, dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db2.locations on locations.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 10
          |group by depts.deptno, dependents.empid
          |""".stripMargin,
      sql =
        """
          |select dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db2.locations on locations.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 11
          |group by dependents.empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 11)
            |group by mv_db.testmv.`empid`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationNoAggregateFuncs8") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select depts.deptno, dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db2.locations on locations.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 20
          |group by depts.deptno, dependents.empid
          |""".stripMargin,
      sql =
        """
          |select dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db2.locations on locations.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 10 and depts.deptno < 20
          |group by dependents.empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db2.dependents.`empid`
            |from db1.emps, db2.dependents, db2.depts, db2.locations
            |where (db1.emps.`deptno` = db2.depts.`deptno`) and (db2.depts.`name` = db2.dependents.`name`) and (db2.locations.`name` = db2.dependents.`name`) and ((db2.depts.`deptno` > 10) AND (db2.depts.`deptno` < 20))
            |group by db2.dependents.`empid`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationNoAggregateFuncs9") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select depts.deptno, dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db2.locations on locations.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 10 and depts.deptno < 20
          |group by depts.deptno, dependents.empid
          |""".stripMargin,
      sql =
        """
          |select dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db2.locations on locations.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 11 and depts.deptno < 19
          |group by dependents.empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where ((mv_db.testmv.`deptno` > 11) AND (mv_db.testmv.`deptno` < 19))
            |group by mv_db.testmv.`empid`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationNoAggregateFuncs10") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("name2", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("deptno2", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select depts.name, dependents.name as name2, emps.deptno, depts.deptno as deptno2, dependents.empid
          |from db2.depts, db2.dependents, db1.emps
          |where depts.deptno > 10
          |group by depts.name, dependents.name, emps.deptno, depts.deptno, dependents.empid
          |""".stripMargin,
      sql =
        """
          |select dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 10
          |group by dependents.empid
          |""".stripMargin)
  }

  test("testJoinAggregateMaterializationAggregateFuncs1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, depts.deptno, count(*) as c, sum(empid) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationAggregateFuncs2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, depts.deptno, count(*) as c, sum(empid) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno, count(*) as c, sum(empid) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |group by depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, sum(mv_db.testmv.`c`) AS `c`, sum(mv_db.testmv.`s`) AS `s`
            |from mv_db.testmv
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationAggregateFuncs3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, depts.deptno, count(*) as c, sum(empid) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, count(*) as c, sum(empid) as s
          |from db1.emps
          |group by empid, deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c`, mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`s` AS `s`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationAggregateFuncs4") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, emps.deptno, count(*) as c, sum(empid) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where emps.deptno >= 10
          |group by empid, emps.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno, sum(empid) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where emps.deptno > 10
          |group by depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, sum(mv_db.testmv.`s`) AS `s`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 10)
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationAggregateFuncs5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("c", LongType, nullable = false)
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, depts.deptno, count(*) + 1 as c, sum(empid) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where emps.deptno >= 10
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno, sum(empid) + 1 as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where depts.deptno > 10
          |group by depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select (sum(mv_db.testmv.`s`) + CAST(1 AS BIGINT)) AS `s`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 10)
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationAggregateFuncs7") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select dependents.empid, emps.deptno, sum(salary) as s
          |from db1.emps join db2.dependents on (emps.empid = dependents.empid)
          |group by dependents.empid, emps.deptno
          |""".stripMargin,
      sql =
        """
          |select dependents.empid, sum(salary) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |join db2.dependents on (emps.empid = dependents.empid)
          |group by dependents.empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`, sum(mv_db.testmv.`s`) AS `s`
            |from db2.depts, mv_db.testmv
            |where (mv_db.testmv.`deptno` = db2.depts.`deptno`)
            |group by mv_db.testmv.`empid`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationAggregateFuncs8") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select dependents.empid, emps.deptno, sum(salary) as s
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |group by dependents.empid, emps.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.name, sum(salary) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |join db2.dependents on emps.empid = dependents.empid
          |group by depts.name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db2.depts.`name` AS `name`, sum(mv_db.testmv.`s`) AS `s`
            |from db2.depts, mv_db.testmv
            |where (mv_db.testmv.`deptno` = db2.depts.`deptno`)
            |group by db2.depts.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationAggregateFuncs9") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select dependents.empid, emps.deptno, count(distinct salary) as s
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |group by dependents.empid, emps.deptno
          |""".stripMargin,
      sql =
        """
          |select emps.deptno, count(distinct salary) as s
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |group by dependents.empid, emps.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`s` AS `s`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationAggregateFuncs10") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select dependents.empid, emps.deptno, count(distinct salary) as s
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |group by dependents.empid, emps.deptno
          |""".stripMargin,
      sql =
        """
          |select emps.deptno, count(distinct salary) as s
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |group by emps.deptno
          |""".stripMargin)
  }

  test("testJoinAggregateMaterializationAggregateFuncs13") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("s", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select dependents.empid, emps.deptno, count(distinct salary) as s
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |group by dependents.empid, emps.deptno
          |""".stripMargin,
      sql =
        """
          |select emps.deptno, count(salary) as s
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |group by dependents.empid, emps.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select count(db1.emps.`salary`) AS `s`, db1.emps.`deptno`
            |from db1.emps, db2.dependents
            |where (db1.emps.`empid` = db2.dependents.`empid`)
            |group by db1.emps.`deptno`, db2.dependents.`empid`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinAggregateMaterializationAggregateFuncs14") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("s", LongType, nullable = false)
        .add("c", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false)
        .add("name1", StringType, nullable = false)
        .add("name2", StringType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, emps.name as name1, emps.deptno, depts.name as name2, count(*) as c, sum(empid) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where (depts.name is not null and emps.name = 'a')
          |   or (depts.name is not null and emps.name = 'b')
          |group by empid, emps.name, depts.name, emps.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno, sum(empid) as s
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where depts.name is not null and emps.name = 'a'
          |group by depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, sum(mv_db.testmv.`s`) AS `s`
            |from mv_db.testmv
            |where ((mv_db.testmv.`name2` IS NOT NULL) AND (mv_db.testmv.`name1` = 'a'))
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterialization4") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, emps.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select empid, depts.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where empid = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` = CAST(1 AS BIGINT))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterialization5") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select cast(empid as BIGINT) as empid
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select empid, depts.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where empid > 1
          |""".stripMargin)
  }

  test("testJoinMaterialization6") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select cast(empid as BIGINT) as empid
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select empid, depts.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where empid = 1
          |""".stripMargin)
  }

  test("testJoinMaterialization7") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false),
      mvQuery =
        """
          |select depts.name
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select dependents.empid
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |join db2.dependents on depts.name = dependents.name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db2.dependents.`empid` AS `empid`
            |from db2.dependents, mv_db.testmv
            |where (mv_db.testmv.`name` = db2.dependents.`name`)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterialization8") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false),
      mvQuery =
        """
          |select depts.name
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db2.dependents.`empid` AS `empid`
            |from db2.dependents, mv_db.testmv
            |where (mv_db.testmv.`name` = db2.dependents.`name`)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterialization9") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false),
      mvQuery =
        """
          |select depts.name
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db2.locations on locations.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db2.dependents.`empid` AS `empid`
            |from db2.dependents, db2.locations, mv_db.testmv
            |where ((mv_db.testmv.`name` = db2.dependents.`name`) AND (db2.locations.`name` = db2.dependents.`name`))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterialization10") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno, dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 30
          |""".stripMargin,
      sql =
        """
          |select dependents.empid
          |from db2.depts join db2.dependents on depts.name = dependents.name
          |join db1.emps on emps.deptno = depts.deptno
          |where depts.deptno > 10
          |""".stripMargin)
  }

  test("testJoinMaterialization12") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("em", StringType, nullable = false)
        .add("dm", StringType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select empid, emps.name as em, emps.deptno, depts.name as dm
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where (depts.name is not null and emps.name = 'a')
          |   or (depts.name is not null and emps.name = 'b')
          |   or (depts.name is not null and emps.name = 'c')
          |""".stripMargin,
      sql =
        """
          |select depts.deptno, depts.name
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where (depts.name is not null and emps.name = 'a')
          |   or (depts.name is not null and emps.name = 'b')
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`dm` AS `dm`
            |from mv_db.testmv
            |where (((mv_db.testmv.`dm` IS NOT NULL) AND (mv_db.testmv.`em` = 'a')) OR ((mv_db.testmv.`dm` IS NOT NULL) AND (mv_db.testmv.`em` = 'b')))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterializationUKFK1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin,
      sql =
        """
          |select emps.empid
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterializationUKFK2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, emps.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin,
      sql =
        """
          |select emps.empid
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterializationUKFK3") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, emps.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin,
      sql =
        """
          |select emps.name
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin)
  }

  test("testJoinMaterializationUKFK4") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, emps.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where emps.empid = 1
          |""".stripMargin,
      sql =
        """
          |select empid
          |from db1.emps
          |where empid = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterializationUKFK5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, emps.deptno
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin,
      sql =
        """
          |select emps.empid
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterializationUKFK6") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, emps.deptno
          |from db1.emps join db2.depts a on emps.deptno = a.deptno
          |join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin,
      sql =
        """
          |select emps.empid
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |where emps.empid = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoinMaterializationUKFK9") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select emps.empid, emps.deptno, emps.name
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |""".stripMargin,
      sql =
        """
          |select emps.empid, dependents.empid, emps.deptno
          |from db1.emps join db2.dependents on emps.empid = dependents.empid
          |join db2.depts a on emps.deptno = a.deptno
          |where emps.name = 'Bill'
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`empid` AS `empid`
            |from db2.depts as a, mv_db.testmv
            |where ((mv_db.testmv.`deptno` = a.`deptno`) AND (mv_db.testmv.`name` = 'Bill'))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testViewMaterialization") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false),
      mvQuery =
        """
          |select depts.name
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.name
          |from db2.depts join db1.emps on emps.deptno = depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testViewSchemaPath") {
    sessionCatalog.setCurrentDatabase("db1")
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false),
      mvQuery =
        """
          |select name, deptno from emps
          |""".stripMargin,
      sql =
        """
          |select deptno from db1.emps
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
    sessionCatalog.setCurrentDatabase("default")
  }

  test("testSingleMaterializationMultiUsage") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select * from db1.emps where empid < 500
          |""".stripMargin,
      sql =
        """
          |select a.name, b.deptno
          |from (select name, empid from db1.emps where empid < 300) a
          |join (select empid, deptno from db1.emps where empid < 200) b on a.empid = b.empid
          |
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select a.`name`, b.`deptno`
            |from (
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` < CAST(200 AS BIGINT))
            |) as `b`, (
            |select mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` < CAST(300 AS BIGINT))
            |) as `a`
            |where (a.`empid` = b.`empid`)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testMaterializationOnJoinQuery") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select * from db1.emps where empid < 500
          |""".stripMargin,
      sql =
        """
          |select *
          |from db1.emps join db2.depts on emps.deptno = depts.deptno
          |where empid < 300
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db2.depts.`deptno` AS `deptno`, db2.depts.`location` AS `location`, db2.depts.`name` AS `name`, mv_db.testmv.`commission` AS `commission`, mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`, mv_db.testmv.`name` AS `name`, mv_db.testmv.`salary` AS `salary`
            |from db2.depts, mv_db.testmv
            |where ((mv_db.testmv.`deptno` = db2.depts.`deptno`) AND (mv_db.testmv.`empid` < CAST(300 AS BIGINT)))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAggregateMaterializationOnCountDistinctQuery1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false)
        .add("salary", DecimalType(10, 2), nullable = false),
      mvQuery =
        """
          |select deptno, empid, salary
          |from db1.emps
          |group by deptno, empid, salary
          |""".stripMargin,
      sql =
        """
          |select deptno, count(distinct empid) as c
          |from (select deptno, empid from db1.emps group by deptno, empid)
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select __auto_generated_subquery_name.`deptno`, count(DISTINCT __auto_generated_subquery_name.`empid`) AS `c`
            |from (
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |group by mv_db.testmv.`deptno`, mv_db.testmv.`empid`
            |) as `__auto_generated_subquery_name`
            |group by __auto_generated_subquery_name.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }
  // scalastyle:on
}
