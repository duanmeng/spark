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

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class MaterializedViewOptimizerSub2Suite extends MaterializedViewOptimizerBaseSuite {

  // scalastyle:off
  // unsupport group by with having
  test("testGroupBy1") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, count(*) as c
          |from db1.emps
          |where deptno > 1
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, count(*) as c1
          |from db1.emps
          |where deptno > 1
          |group by deptno
          |having count(*) > 11
          |""".stripMargin)
  }

  test("testGroupBy2") {
    withUnSatisfiedMV("testmv",
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
          |select deptno
          |from db1.emps
          |""".stripMargin)
  }

  test("testGroupBy3") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |group by deptno, empid
          |""".stripMargin)
  }

  // unsupport group by with having in mv, it will failed as "Invalid alias name"
  test("testGroupBy4") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, count(*) as c
          |from db1.emps
          |where deptno > 1
          |group by deptno
          |having count(*) > 11
          |""".stripMargin,
      sql =
        """
          |select deptno, count(*) as c1
          |from db1.emps
          |where deptno > 1
          |group by deptno
          |having count(*) > 11
          |""".stripMargin)
  }

  // unsupport group by with having in query, it will failed as "output list is not satisfied"
  test("testGroupBy5") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, count(*) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, count(*) as c1
          |from db1.emps
          |group by deptno
          |having count(*) > 11
          |""".stripMargin)
  }

  // expression in group by, for both mv and query
  test("testGroupBy6") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("subn", StringType)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select substr(name, 1, 20) as subn, deptno, count(*) as c
          |from db1.emps
          |group by substr(name, 1, 20), deptno
          |""".stripMargin,
      sql =
        """
          |select substr(name, 1, 20) as subn, count(*) as c
          |from db1.emps
          |group by substr(name, 1, 20)
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`subn` AS `subn`, sum(mv_db.testmv.`c`) AS `c`
            |from mv_db.testmv
            |group by mv_db.testmv.`subn`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  // expression in group by of query
  test("testGroupBy7") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select name, count(*) as c
          |from db1.emps
          |group by name
          |""".stripMargin,
      sql =
        """
          |select substr(name, 1, 20) as subn, count(*) as c
          |from db1.emps
          |group by substr(name, 1, 20)
          |""".stripMargin)
  }

  // expression in group by
  test("testGroupBy8") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType),
      mvQuery =
        """
          |select name, deptno
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select substr(name, 1, 20) as subn, count(*) as c
          |from db1.emps
          |group by substr(name, 1, 20)
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select count(1) AS `c`, substr(mv_db.testmv.`name`, 1, 20) AS `subn`
            |from mv_db.testmv
            |group by substr(mv_db.testmv.`name`, 1, 20)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupBy9") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("c", LongType),
      mvQuery =
        """
          |select count(*) as c
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select count(*) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin)
  }

  // query can be materialized with view, but view has less records then query, unsatisfied
  test("testGroupBy10") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("c", LongType),
      mvQuery =
        """
          |select count(*) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select count(*) as c1
          |from db1.emps
          |""".stripMargin)
  }

  test("testGroupBy11") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select e.deptno
          |from db1.emps e
          |group by e.deptno
          |""".stripMargin,
      sql =
        """
          |select e.deptno
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |group by e.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from db2.depts as d, mv_db.testmv
            |where (mv_db.testmv.`deptno` = d.`deptno`)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupBy12") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select e.deptno, d.name, count(d.name) as c
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |group by e.deptno, d.name
          |""".stripMargin,
      sql =
        """
          |select e.deptno, count(d.name)
          |from  db2.depts d join db1.emps e on e.deptno = d.deptno
          |join db2.dependents on d.name = dependents.name
          |group by e.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, sum(mv_db.testmv.`c`) AS `count(name)`
            |from db2.dependents, mv_db.testmv
            |where (mv_db.testmv.`name` = db2.dependents.`name`)
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupBy13") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select e.deptno
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |group by e.deptno, d.name
          |""".stripMargin,
      sql =
        """
          |select d.deptno
          |from  db2.depts d join db1.emps e on e.deptno = d.deptno
          |group by d.deptno
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

  test("testGroupBy14") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select e.deptno
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |group by e.deptno, d.name
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from (
          |select d.deptno
          |from  db2.depts d join db1.emps e on e.deptno = d.deptno
          |where d.deptno = 1
          |group by d.deptno) t
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select t.`deptno`
            |from (
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 1)
            |group by mv_db.testmv.`deptno`
            |) as t
            |group by t.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testWith1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select e.deptno
          |from db1.emps e join db2.depts d on e.deptno = d.deptno
          |group by e.deptno, d.name
          |""".stripMargin,
      sql =
        """
          |with t as
          |(select d.deptno
          |from  db2.depts d join db1.emps e on e.deptno = d.deptno
          |where d.deptno = 1
          |group by d.deptno)
          |select deptno
          |from t
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select t.`deptno`
            |from (
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 1)
            |group by mv_db.testmv.`deptno`
            |) as t
            |group by t.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunCount1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, count(*) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, count(*) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunCount2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, count(*) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, count(*) as c
          |from db1.emps
          |group by name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `name`, sum(mv_db.testmv.`c`) AS `c`
            |from mv_db.testmv
            |group by mv_db.testmv.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunCount3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, count(*) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, count(deptno) as c
          |from db1.emps
          |group by name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select count(mv_db.testmv.`deptno`) AS `c`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |group by mv_db.testmv.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunCount4") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, count(*) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, count(distinct name, deptno) as c
          |from db1.emps
          |group by name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select count(DISTINCT mv_db.testmv.`name`, mv_db.testmv.`deptno`) AS `c`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |group by mv_db.testmv.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunCount5") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, count(*) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, count(distinct name, deptno) as c
          |from db1.emps
          |group by name, deptno
          |""".stripMargin)
  }

  test("testGroupByAggregateFunCount6") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, count(commission, salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, count(commission, salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunCount7") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, count(commission, salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, count(commission, salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `name`, sum(mv_db.testmv.`c`) AS `c`
            |from mv_db.testmv
            |group by mv_db.testmv.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunSum1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, sum(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, sum(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunSum2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, sum(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, sum(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `name`, sum(mv_db.testmv.`c`) AS `c`
            |from mv_db.testmv
            |group by mv_db.testmv.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunMin1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, min(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, min(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunMin2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, min(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, min(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select min(mv_db.testmv.`c`) AS `c`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |group by mv_db.testmv.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunMax1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, max(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, max(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunMax2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, max(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, max(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select max(mv_db.testmv.`c`) AS `c`, mv_db.testmv.`name` AS `name`
            |from mv_db.testmv
            |group by mv_db.testmv.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunAvg1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, avg(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, avg(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateFunAvg2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, avg(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, avg(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin)
  }

  test("testGroupByAggregateFunVarsamp1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, var_samp(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, var_samp(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateVarsamp2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, var_samp(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, var_samp(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin)
  }

  test("testGroupByAggregateFunStdevpop1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, stddev_pop(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, stddev_pop(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateStdevpop2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, stddev_pop(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, stddev_pop(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin)
  }

  test("testGroupByAggregateFunVariancePop1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, var_pop(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, var_pop(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateVariancePop2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, var_pop(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, var_pop(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin)
  }

  test("testGroupByAggregateFunSkewness1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, skewness(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, skewness(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateSkewness2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, skewness(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, skewness(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin)
  }

  test("testGroupByAggregateFunStddevSamp1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, stddev_samp(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, stddev_samp(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateStddevSamp2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, stddev_samp(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, stddev_samp(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin)
  }

  test("testGroupByAggregateFunKurtosis1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, kurtosis(salary) as c
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno, kurtosis(salary) as c1
          |from db1.emps
          |group by deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`c` AS `c1`, mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testGroupByAggregateKurtosis2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("name", StringType, nullable = false)
        .add("c", LongType),
      mvQuery =
        """
          |select deptno, name, kurtosis(salary) as c
          |from db1.emps
          |group by deptno, name
          |""".stripMargin,
      sql =
        """
          |select name, kurtosis(salary) as c
          |from db1.emps
          |group by name
          |""".stripMargin)
  }

  test("testDistinct1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select distinct deptno, empid
          |from db1.emps
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select distinct mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  // distinct = groupby, it's the same result, but doesn't support yet
  test("testDistinct2") {
    withUnSatisfiedMV("testmv",
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
          |select distinct deptno
          |from db1.emps
          |""".stripMargin)
  }

  // unsupport distinct in mv
  test("testDistinct3") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select distinct deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select distinct deptno, empid
          |from db1.emps
          |""".stripMargin)
  }

  // self join is not supported
  test("testJoin1") {
    withUnSatisfiedMV("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select *
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select e1.deptno, e2.empid
          |from db1.emps e1, db1.emps e2
          |where e1.empid = e2.empid
          |""".stripMargin)
  }

  // self join is not supported
  test("testJoin2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select e1.deptno, e2.empid
          |from db1.emps e1, db1.emps e2
          |where e1.empid = e2.empid
          |""".stripMargin,
      sql =
        """
          |select *
          |from db1.emps
          |""".stripMargin)
  }

  // for query, col equal expressions can't connect all table
  test("testJoin3") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations, db1.emps
          |where depts.name = dependents.name
          |  and locations.name = dependents.name
          |  and emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations, db1.emps
          |where locations.name = dependents.name
          |and emps.deptno = depts.deptno
          |""".stripMargin)
  }

  // for query, col equal expressions can't connect all table
  test("testJoin4") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations, db1.emps
          |where depts.name = dependents.name
          |  and locations.name = dependents.name
          |  and emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations, db1.emps
          |where locations.name = dependents.name
          |and emps.deptno = depts.deptno
          |and emps.deptno = depts.deptno
          |""".stripMargin)
  }

  // for mv, col equal expressions can't connect all table
  test("testJoin5") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations, db1.emps
          |where locations.name = dependents.name
          |and emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations, db1.emps
          |where depts.name = dependents.name
          |  and locations.name = dependents.name
          |  and emps.deptno = depts.deptno
          |""".stripMargin)
  }

  // for mv, col equal expressions can't connect all table
  test("testJoin6") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations, db1.emps
          |where locations.name = dependents.name
          |and emps.deptno = depts.deptno
          |and emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations, db1.emps
          |where depts.name = dependents.name
          |  and locations.name = dependents.name
          |  and emps.deptno = depts.deptno
          |""".stripMargin)
  }

  // with PFModelEnabled = false
  test("testJoin7") {
    withUnSatisfiedMV("testmv",
      isPFModelEnabled = false,
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations, db1.emps
          |where depts.name = dependents.name
          |and locations.name = dependents.name
          |and emps.deptno = depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations
          |where depts.name = dependents.name
          |  and locations.name = dependents.name
          |""".stripMargin)
  }

  // unsupported join
  test("testJoin8") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db2.locations
          |where depts.name = dependents.name
          |and locations.name = dependents.name
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts, db2.dependents, db1.emps
          |where depts.name = dependents.name
          |  and emps.deptno = depts.deptno
          |""".stripMargin)
  }

  test("testJoin9") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select e.deptno, d.name
          |from db1.emps e join db2.depts d
          |where e.deptno = d.deptno
          |order by d.name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select d.`name` AS `name`, mv_db.testmv.`deptno` AS `deptno`
            |from db2.depts as d, mv_db.testmv
            |where (mv_db.testmv.`deptno` = d.`deptno`)
            |order by d.`name` ASC NULLS FIRST
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoin10") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select d.name
          |from db1.emps e join db2.depts d
          |where e.deptno = d.deptno
          |group by d.name
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select d.`name` AS `name`
            |from db2.depts as d, mv_db.testmv
            |where (mv_db.testmv.`deptno` = d.`deptno`)
            |group by d.`name`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoin11") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select e.deptno, d.name
          |from db1.emps e join db2.depts d
          |where e.deptno = d.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select d.`name` AS `name`, mv_db.testmv.`deptno` AS `deptno`
            |from db2.depts as d, mv_db.testmv
            |where (mv_db.testmv.`deptno` = d.`deptno`)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoin12") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select e.deptno, d.name
          |from db1.emps e join db2.depts d
          |where e.deptno = d.deptno
          |  and d.deptno > 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select d.`name` AS `name`, mv_db.testmv.`deptno` AS `deptno`
            |from db2.depts as d, mv_db.testmv
            |where ((mv_db.testmv.`deptno` = d.`deptno`) AND (d.`deptno` > 1))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testJoin13") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select e.deptno, d.name
          |from db1.emps e join db2.depts d
          |where e.deptno = d.deptno
          |  and d.deptno > 1
          |  and e.deptno > 0
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select d.`name` AS `name`, mv_db.testmv.`deptno` AS `deptno`
            |from db2.depts as d, mv_db.testmv
            |where ((mv_db.testmv.`deptno` = d.`deptno`) AND ((mv_db.testmv.`deptno` > 0) AND (d.`deptno` > 1)))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testSort1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select deptno, empid
          |from db1.emps
          |order by deptno, empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |order by mv_db.testmv.`deptno` ASC NULLS FIRST, mv_db.testmv.`empid` ASC NULLS FIRST
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testSort2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select deptno, empid
          |from db1.emps
          |order by deptno asc, empid + 1 desc
          |""".stripMargin)
  }

  test("testSort3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |(select deptno, empid
          |from db1.emps
          |order by deptno asc, empid + 1 desc)
          |union all
          |select deptno, empid
          |from db1.emps
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select db1.emps.`deptno`, db1.emps.`empid`
            |from db1.emps
            |order by (db1.emps.`empid` + CAST(1 AS BIGINT)) DESC NULLS LAST, db1.emps.`deptno` ASC NULLS FIRST
            |union all
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testSort4") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select deptno as d, empid as e
          |from db1.emps
          |order by deptno, empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `d`, mv_db.testmv.`empid` AS `e`
            |from mv_db.testmv
            |order by mv_db.testmv.`deptno` ASC NULLS FIRST, mv_db.testmv.`empid` ASC NULLS FIRST
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testSort5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select empid
          |from db1.emps
          |group by empid
          |order by empid
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |group by mv_db.testmv.`empid`
            |order by `empid` ASC NULLS FIRST
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  // todo: it should be supported
  test("testSort6") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select empid as e, deptno as d
          |from db1.emps
          |group by empid, deptno
          |order by empid, deptno
          |""".stripMargin)
  }

  test("testSort7") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select deptno as d, empid
          |from db1.emps
          |order by d
          |""".stripMargin)
  }

  test("testSort8") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select deptno as d, empid as e
          |from db1.emps
          |order by deptno + 1
          |""".stripMargin)
  }

  test("testSort9") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select deptno as d
          |from db1.emps
          |group by deptno
          |order by deptno + 1
          |""".stripMargin)
  }

  test("testSort10") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, empid
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select empid, deptno
          |from db1.emps
          |group by empid, deptno
          |order by empid, deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`, mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |group by mv_db.testmv.`deptno`, mv_db.testmv.`empid`
            |order by `deptno` ASC NULLS FIRST, `empid` ASC NULLS FIRST
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter1") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery = "select * from db1.emps where deptno = 10",
      sql = "select emps.empid from db1.emps where deptno = 10") {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter2") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery = "select * from db1.emps",
      sql = "select emps.empid from db1.emps where deptno = 10") {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`empid` AS `empid`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 10)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("empid", LongType, nullable = false),
      mvQuery =
        """
          |select depts.deptno, empid
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |group by empid, depts.deptno
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno and emps.empid = 15
          |group by depts.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`empid` = CAST(15 AS BIGINT))
            |group by mv_db.testmv.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter4") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db2.depts
          |where deptno between 1 and 10
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |where deptno between 2 and 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` >= 2)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  // In is unsupported for scope match
  test("testFilter5") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db2.depts
          |where deptno between 1 and 10
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |where deptno in (2, 10)
          |""".stripMargin)
  }

  test("testFilter6") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |where name in ('a', 'b')
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`name` IN ('a', 'b'))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter7") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |where deptno > 100 or (deptno < 1 and (deptno > -1 or deptno = -100))
          |""".stripMargin)
  }

  test("testFilter8") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |where deptno > 100 or (deptno < 1 and (deptno > -1 or deptno = -100))
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |""".stripMargin)
  }

  test("testFilter9") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |where deptno != 100
          |""".stripMargin)
  }

  test("testFilter10") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |where deptno != 100
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |""".stripMargin)
  }

  test("testFilter11") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |where not deptno = 100
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |""".stripMargin)
  }

  test("testFilter12") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |where not deptno = 100
          |""".stripMargin)
  }

  test("testFilter13") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |where deptno != 100 or (deptno < 1 and deptno > -1)
          |""".stripMargin)
  }

  test("testFilter14") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |where deptno = 100 or (deptno < 1 and deptno != -1)
          |""".stripMargin)
  }

  test("testFilter15") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db2.depts
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db2.depts
          |where deptno = 100 or (deptno < 1 and deptno > -10 and deptno != -1)
          |""".stripMargin)
  }

  test("testFilter16") {
    withUnSatisfiedMV("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select *
          |from db1.emps
          |where deptno > 100
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno > empid + 1
          |""".stripMargin)
  }

  test("testFilter17") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select *
          |from db1.emps
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno > empid + 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (CAST(mv_db.testmv.`deptno` AS BIGINT) > (mv_db.testmv.`empid` + CAST(1 AS BIGINT)))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter18") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where deptno > 0
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 1)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter19") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where deptno > 0
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno > 2
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 2)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter20") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where deptno > 0
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno < 2
          |""".stripMargin)
  }

  test("testFilter21") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where deptno >= 0 and deptno < 10
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno = 0
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 0)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter22") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where deptno >= 0 and deptno < 10
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno > 1 and deptno < 9
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where ((mv_db.testmv.`deptno` > 1) AND (mv_db.testmv.`deptno` < 9))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter23") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where deptno >= 0 and deptno < 10
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno > 1 and deptno < 10
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 1)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter24") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where deptno >= 0 and deptno < 10
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno > 1 and deptno <= 10
          |""".stripMargin)
  }

  test("testFilter25") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where (deptno >= 0 and deptno < 10)
          |   or (deptno > 10 and deptno <= 20)
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno = 20
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` = 20)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter26") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where (deptno >= 0 and deptno < 10)
          |   or (deptno > 10 and deptno <= 20)
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno = 20 and deptno > 20
          |""".stripMargin)
  }

  test("testFilter27") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where (deptno >= 0 and deptno < 10)
          |   or (deptno > 10 and deptno <= 20)
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where deptno = 10
          |""".stripMargin)
  }

  test("testFilter28") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where (deptno >= 0 and deptno < 10)
          |   or (deptno > 10 and deptno <= 20)
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where (deptno >= 0 and deptno < 10)
          |   or (deptno > 10 and deptno <= 20)
          |   or (deptno >= 1 and deptno < 9)
          |""".stripMargin)  {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where ((((mv_db.testmv.`deptno` >= 0) AND (mv_db.testmv.`deptno` < 10)) OR ((mv_db.testmv.`deptno` > 10) AND (mv_db.testmv.`deptno` <= 20))) OR ((mv_db.testmv.`deptno` >= 1) AND (mv_db.testmv.`deptno` < 9)))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter30") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where (deptno >= 0 and deptno < 10)
          |   or (deptno > 10 and deptno <= 20)
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |""".stripMargin)
  }

  test("testFilter31") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno
          |from db1.emps
          |where 2 > 1
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where 2 > 1
          |""".stripMargin)  {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testFilter32") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db1.emps
          |where name = 'name'
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |where concat_ws(name, '_', name) = 'name'
          |""".stripMargin)
  }

  test("testFilter33") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("name", StringType, nullable = false)
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select deptno, name
          |from db1.emps
          |where concat_ws(name, '_', '2') = 'name'
          |""".stripMargin,
      sql =
        """
          |select deptno, name
          |from db1.emps
          |where concat_ws(name, '2', '_') = 'name'
          |""".stripMargin)
  }

  test("testEC1") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |  and depts.deptno = 1
          |""".stripMargin,
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          | and emps.deptno = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  // emps.deptno = depts.deptno and emps.deptno = 1 will be parsed as
  // 1 = depts.deptno and emps.deptno = 1
  test("testEC2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |  and depts.deptno > 0
          |""".stripMargin,
      sql =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          | and emps.deptno = 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where ((1 = mv_db.testmv.`deptno`) AND (mv_db.testmv.`deptno` = 1))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testEC3") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |  and depts.deptno > 0
          |""".stripMargin,
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          | and emps.deptno > 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 1)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testEC4") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |  and depts.deptno > 0
          |""".stripMargin,
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          | and emps.deptno < 1
          |""".stripMargin)
  }

  test("testEC5") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |  and depts.deptno > 0
          |""".stripMargin,
      sql =
        """
          |select abs(emps.deptno) as d
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          | and emps.deptno > 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select abs(mv_db.testmv.`deptno`) AS `d`
            |from mv_db.testmv
            |where (mv_db.testmv.`deptno` > 1)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testEC6") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |  and abs(depts.deptno) > 1
          |""".stripMargin,
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          | and abs(emps.deptno) > 1
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testEC7") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |group by depts.deptno
          |""".stripMargin,
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |group by emps.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testEC8") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery =
        """
          |select depts.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |group by depts.deptno
          |""".stripMargin,
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |group by emps.deptno
          |order by emps.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`deptno` AS `deptno`
            |from mv_db.testmv
            |order by mv_db.testmv.`deptno` ASC NULLS FIRST
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAlias") {
    withMaterializedView("testmv",
      mvSchema = empsStruct,
      mvQuery =
        """
          |select *
          |from db1.emps as em
          |where (em.salary < 1111.9 and em.deptno > 10)
          |  or (em.empid > 400 and em.salary > 5000)
          |""".stripMargin,
      sql =
        """
          |select name as n
          |from db1.emps as e
          |where e.empid > 500 and e.salary > 6000
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`name` AS `n`
            |from mv_db.testmv
            |where ((mv_db.testmv.`empid` > CAST(500 AS BIGINT)) AND (CAST(mv_db.testmv.`salary` AS DECIMAL(10,2)) > CAST(CAST(6000 AS DECIMAL(4,0)) AS DECIMAL(10,2))))
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("testAlias2") {
    withMaterializedView("testmv",
      mvSchema = new StructType()
        .add("md", IntegerType, nullable = false),
      mvQuery =
        """
          |select dd.deptno as md
          |from db2.depts dd join db1.emps de
          |where de.deptno = dd.deptno
          |group by dd.deptno
          |""".stripMargin,
      sql =
        """
          |select qed.deptno as ed
          |from db2.depts qdd join db1.emps qed
          |where qed.deptno = qdd.deptno
          |group by qed.deptno
          |""".stripMargin) {
      materialized =>

        assert(
          """
            |select mv_db.testmv.`md` AS `ed`
            |from mv_db.testmv
            |""".stripMargin.equals(getSql(materialized)))
    }
  }

  test("invalidMaterializedView1") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, count(*)
          |from db1.emps
          |group by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |group by deptno
          |""".stripMargin)
  }

  test("invalidMaterializedView2") {
    withUnSatisfiedMV("testmv",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false)
        .add("c", LongType, nullable = false),
      mvQuery =
        """
          |select deptno, count(*) as c
          |from db1.emps
          |group by deptno
          |order by deptno
          |""".stripMargin,
      sql =
        """
          |select deptno
          |from db1.emps
          |group by deptno
          |""".stripMargin)
  }

  test("testMultipleMatch1") {
    val tmv1 = new TestMaterializedView(
      mvDb = MATERIALIZED_VIEW_DB,
      mvName = "tmv1",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery = """
                  |select depts.deptno
                  |from db2.depts join db1.emps
                  |where emps.deptno = depts.deptno
                  |group by depts.deptno
                  |""".stripMargin
    )

    val tmv2 = new TestMaterializedView(
      mvDb = MATERIALIZED_VIEW_DB,
      mvName = "tmv2",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery = """
                  |select depts.deptno
                  |from db2.depts join db1.emps
                  |where emps.deptno = depts.deptno
                  |""".stripMargin
    )

    val tmv3 = new TestMaterializedView(
      mvDb = MATERIALIZED_VIEW_DB,
      mvName = "tmv3",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery = """
                  |select deptno
                  |from db1.emps
                  |""".stripMargin
    )

    val tmv4 = new TestMaterializedView(
      mvDb = MATERIALIZED_VIEW_DB,
      mvName = "tmv4",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery = """
                  |select depts.deptno
                  |from db2.depts join db1.emps
                  |where emps.deptno = depts.deptno
                  |  and emps.deptno > 0
                  |group by depts.deptno
                  |""".stripMargin
    )

    val tmv5 = new TestMaterializedView(
      mvDb = MATERIALIZED_VIEW_DB,
      mvName = "tmv5",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery = """
                  |select depts.deptno
                  |from db2.depts join db1.emps
                  |where emps.deptno = depts.deptno
                  |  and emps.deptno > 0
                  |""".stripMargin
    )

    val ftmv1 = new TestMaterializedView(
      mvDb = MATERIALIZED_VIEW_DB,
      mvName = "ftmv1",
      mvSchema = new StructType()
        .add("deptno", IntegerType, nullable = false),
      mvQuery = """
                  |select deptno
                  |from db2.depts
                  |where deptno < 0
                  |""".stripMargin
    )

    withMultipleMaterializedView(mvs = Seq(tmv3, ftmv1),
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |group by emps.deptno
          |""".stripMargin) {
      materialized =>
        assert(
          """
            |select mv_db.tmv3.`deptno` AS `deptno`
            |from db2.depts, mv_db.tmv3
            |where (mv_db.tmv3.`deptno` = db2.depts.`deptno`)
            |group by mv_db.tmv3.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }

    // tmv2 is the best one
    withMultipleMaterializedView(mvs = Seq(tmv2, tmv3, ftmv1),
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |group by emps.deptno
          |""".stripMargin) {
      materialized =>
        assert(
          """
            |select mv_db.tmv2.`deptno` AS `deptno`
            |from mv_db.tmv2
            |group by mv_db.tmv2.`deptno`
            |""".stripMargin.equals(getSql(materialized)))
    }

    // tmv1 is the best one
    withMultipleMaterializedView(mvs = Seq(tmv1, tmv2, tmv3),
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |group by emps.deptno
          |""".stripMargin) {
      materialized =>
        assert(
          """
            |select mv_db.tmv1.`deptno` AS `deptno`
            |from mv_db.tmv1
            |""".stripMargin.equals(getSql(materialized)))
    }

    // tmv4 should be the best one, but spark.sql.materializedView.multiplePolicy.limit = 3
    // after (tmv1, tmv2, tmv3) matched, tmv4 won't be considered
    withMultipleMaterializedView(mvs = Seq(tmv1, tmv2, tmv3, tmv4),
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |  and emps.deptno > 0
          |group by emps.deptno
          |""".stripMargin) {
      materialized =>
        assert(
          """
            |select mv_db.tmv1.`deptno` AS `deptno`
            |from mv_db.tmv1
            |where (mv_db.tmv1.`deptno` > 0)
            |""".stripMargin.equals(getSql(materialized)))
    }

    // ftmv1 is unmatched, tmv4 is the best one in (tmv1, tmv2, tmv4)
    withMultipleMaterializedView(mvs = Seq(tmv1, tmv2, ftmv1, tmv4),
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |  and emps.deptno > 0
          |group by emps.deptno
          |""".stripMargin) {
      materialized =>
        assert(
          """
            |select mv_db.tmv4.`deptno` AS `deptno`
            |from mv_db.tmv4
            |""".stripMargin.equals(getSql(materialized)))
    }

    // tmv1 is the better one, tmv5 cover filter but has extra group by
    withMultipleMaterializedView(mvs = Seq(tmv1, tmv5),
      sql =
        """
          |select emps.deptno
          |from db2.depts join db1.emps
          |where emps.deptno = depts.deptno
          |  and emps.deptno > 0
          |group by emps.deptno
          |""".stripMargin) {
      materialized =>
        assert(
          """
            |select mv_db.tmv1.`deptno` AS `deptno`
            |from mv_db.tmv1
            |where (mv_db.tmv1.`deptno` > 0)
            |""".stripMargin.equals(getSql(materialized)))
    }
  }
  // scalastyle:on
}
