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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSparkSession

class RecordFunctionSuite extends QueryTest with SharedSparkSession {

  test("record_every") {
    val df: DataFrame = spark.read.json(testFile("test-data/nest-test-data.json"))

    checkAnswer(
      df.selectExpr("record_every(r1.m2.f3, '>', 2)"),
      Row(false) :: Row(true) :: Nil
    )

    checkAnswer(
      df.selectExpr("record_every(m1.f2, '<>', 'a')"),
      Row(false) :: Row(true) :: Nil
    )
  }

  test("record_some") {
    val df: DataFrame = spark.read.json(testFile("test-data/nest-test-data.json"))

    checkAnswer(
      df.selectExpr("record_some(r1.m2.f3, '>', 2)"),
      Row(true) :: Row(true) :: Nil
    )

    checkAnswer(
      df.selectExpr("record_some(m1.f2, '<>', 'a')"),
      Row(true) :: Row(true) :: Nil
    )
  }

  test("record_count") {
    val df: DataFrame = spark.read.json(testFile("test-data/nest-test-data.json"))
    checkAnswer(
      df.selectExpr("record_count(r1.m2.f3)"),
      Row(3) :: Row(4) :: Nil
    )
  }

}
