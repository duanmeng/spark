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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.types.{Decimal, IntegerType}

class MaterializedViewUtilSuite extends SparkFunSuite {

  // qcvs: [QL, QH]
  // vcvs: [VL, VH]
  // QH > VH:
  //    QL > VH: [QL, QH]
  //    QL = VH: [QL, QH]
  //    QL < VH:
  //       QL > VL: [VH, QH]
  //       QL = VL: [VH, QH]
  //       QL < VL: [VH, QH] and [QL, VL]
  // QH = VH:
  //    QL > VL: [VL, QL], maybe [QH, QH]
  //    QL = VL: [QL, QL], [QH, QH](one, both, empty)
  //    QL < VL: [] or [QH, QH]
  // QH < VH:
  //    VL > QH: [QL, QH]
  //    VL = QH: [QL, QH]
  //    VL < QH:
  //       VL > QL: [QH, VH]
  //       VL = QL: [QH, VH]
  //       VL < QL: []
  test("testCompensateValueScopes") {
    val mvu = MaterializedViewUtil
    // QH > VH, QL > VH
    var compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, true),
      vcvs = new ColValueScope(Some(-10), Some(-1), true, true))
    assert(compensates.size == 1)
    var rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), None, true, false),
      vcvs = new ColValueScope(None, Some(-1), false, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.isEmpty
      && rcs.includeLowValue && !rcs.includeHighValue)

    // QH > VH, QL = VH
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, true),
      vcvs = new ColValueScope(Some(-10), Some(1), true, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && !rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), None, true, false),
      vcvs = new ColValueScope(None, Some(1), false, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.isEmpty
      && !rcs.includeLowValue && !rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), false, true),
      vcvs = new ColValueScope(Some(-10), Some(1), true, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && !rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, true),
      vcvs = new ColValueScope(Some(-10), Some(1), true, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), false, true),
      vcvs = new ColValueScope(Some(-10), Some(1), true, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && !rcs.includeLowValue && rcs.includeHighValue)

    // QH > VH, QL < VH, QL > VL
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(0), Some(10), false, true),
      vcvs = new ColValueScope(Some(-10), Some(1), true, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(0), None, false, false),
      vcvs = new ColValueScope(None, Some(1), false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.isEmpty
      && rcs.includeLowValue && !rcs.includeHighValue)

    // QH > VH, QL < VH, QL = VL
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(0), Some(10), false, true),
      vcvs = new ColValueScope(Some(0), Some(1), true, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(None, None, false, false),
      vcvs = new ColValueScope(None, Some(1), false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.isEmpty
      && rcs.includeLowValue && !rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(0), Some(10), true, true),
      vcvs = new ColValueScope(Some(0), Some(1), true, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(0), Some(10), false, true),
      vcvs = new ColValueScope(Some(0), Some(1), false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(0), Some(10), true, true),
      vcvs = new ColValueScope(Some(0), Some(1), false, false))
    assert(compensates.size == 2)
    compensates.foreach(c => assert(
      (c.lowValue.get == 1 && c.highValue.get == 10 && c.includeLowValue && c.includeHighValue)
        || (c.lowValue.get == 0 && c.highValue.get == 0
        && c.includeLowValue && c.includeHighValue)))

    // QH > VH, QL < VH, QL < VL
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(0), Some(10), true, true),
      vcvs = new ColValueScope(Some(1), Some(2), false, false))
    assert(compensates.size == 2)
    compensates.foreach(c => assert(
      (c.lowValue.get == 0 && c.highValue.get == 1 && c.includeLowValue && c.includeHighValue)
        || (c.lowValue.get == 2 && c.highValue.get == 10
        && c.includeLowValue && c.includeHighValue)))

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(None, None, false, false),
      vcvs = new ColValueScope(Some(1), Some(2), false, false))
    assert(compensates.size == 2)
    compensates.foreach(c =>
      if (c.lowValue.isEmpty) {
        assert(c.lowValue.isEmpty && c.highValue.get == 1
          && !c.includeLowValue && c.includeHighValue)
      } else {
        assert(c.lowValue.get == 2 && c.highValue.isEmpty
          && c.includeLowValue && !c.includeHighValue)
      }
    )

    // QH = VH, QL > VL
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), false, true),
      vcvs = new ColValueScope(Some(0), Some(10), false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 10 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), None, false, false),
      vcvs = new ColValueScope(Some(0), None, false, false))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), false, true),
      vcvs = new ColValueScope(Some(0), Some(10), false, true))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), false, false),
      vcvs = new ColValueScope(Some(0), Some(10), false, false))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), false, false),
      vcvs = new ColValueScope(Some(0), Some(10), false, true))
    assert(compensates.isEmpty)

    // QH = VH, QL = VL
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), false, false),
      vcvs = new ColValueScope(Some(1), Some(10), true, true))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(None, Some(10), false, false),
      vcvs = new ColValueScope(None, Some(10), true, true))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), None, false, false),
      vcvs = new ColValueScope(Some(1), None, true, false))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(None, None, false, false),
      vcvs = new ColValueScope(None, None, true, false))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, true),
      vcvs = new ColValueScope(Some(1), Some(10), true, true))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), false, false),
      vcvs = new ColValueScope(Some(1), Some(10), false, false))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, false),
      vcvs = new ColValueScope(Some(1), Some(10), false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 1
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, true),
      vcvs = new ColValueScope(Some(1), Some(10), true, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 10 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(1), true, true),
      vcvs = new ColValueScope(Some(1), Some(1), false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 1
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(1), false, true),
      vcvs = new ColValueScope(Some(1), Some(1), false, false))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(1), true, false),
      vcvs = new ColValueScope(Some(1), Some(1), false, false))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, true),
      vcvs = new ColValueScope(Some(1), Some(10), false, false))
    assert(compensates.size == 2)
    compensates.foreach(c => assert(
      (c.lowValue.get == 1 && c.highValue.get == 1 && c.includeLowValue && c.includeHighValue)
        || (c.lowValue.get == 10 && c.highValue.get == 10
        && c.includeLowValue && c.includeHighValue)))

    // QH = VH, QL < VL
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, true),
      vcvs = new ColValueScope(Some(2), Some(10), false, false))
    assert(compensates.size == 2)
    compensates.foreach(c => assert(
      (c.lowValue.get == 1 && c.highValue.get == 2 && c.includeLowValue && c.includeHighValue)
        || (c.lowValue.get == 10 && c.highValue.get == 10
        && c.includeLowValue && c.includeHighValue)))

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, false),
      vcvs = new ColValueScope(Some(2), Some(10), false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 2
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), None, true, false),
      vcvs = new ColValueScope(Some(2), None, false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 2
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(None, None, false, false),
      vcvs = new ColValueScope(Some(2), None, false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.isEmpty && rcs.highValue.get == 2
      && !rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, true),
      vcvs = new ColValueScope(Some(2), Some(10), false, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 2
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(10), true, false),
      vcvs = new ColValueScope(Some(2), Some(10), false, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 2
      && rcs.includeLowValue && rcs.includeHighValue)

    // QH < VH, VL > QH
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), true, false),
      vcvs = new ColValueScope(Some(6), Some(10), false, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 5
      && rcs.includeLowValue && !rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(None, Some(5), false, false),
      vcvs = new ColValueScope(Some(6), None, false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.isEmpty && rcs.highValue.get == 5
      && !rcs.includeLowValue && !rcs.includeHighValue)

    // QH < VH, VL = QH
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), true, false),
      vcvs = new ColValueScope(Some(5), Some(10), false, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 5
      && rcs.includeLowValue && !rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(None, Some(5), false, false),
      vcvs = new ColValueScope(Some(5), None, false, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.isEmpty && rcs.highValue.get == 5
      && !rcs.includeLowValue && !rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), true, true),
      vcvs = new ColValueScope(Some(5), Some(10), false, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 5
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), true, false),
      vcvs = new ColValueScope(Some(5), Some(10), true, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 5
      && rcs.includeLowValue && !rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), true, true),
      vcvs = new ColValueScope(Some(5), Some(10), true, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 5
      && rcs.includeLowValue && !rcs.includeHighValue)

    // QH < VH, VL < QH, VL > QL
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), true, true),
      vcvs = new ColValueScope(Some(4), Some(10), true, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 4
      && rcs.includeLowValue && !rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(None, Some(5), false, true),
      vcvs = new ColValueScope(Some(4), None, true, false))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.isEmpty && rcs.highValue.get == 4
      && !rcs.includeLowValue && !rcs.includeHighValue)

    // QH < VH, VL < QH, VL = QL
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), true, true),
      vcvs = new ColValueScope(Some(1), Some(10), false, true))
    assert(compensates.size == 1)
    rcs = compensates(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 1
      && rcs.includeLowValue && rcs.includeHighValue)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), true, true),
      vcvs = new ColValueScope(Some(1), Some(10), true, true))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(None, Some(5), false, true),
      vcvs = new ColValueScope(None, Some(10), false, true))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), false, true),
      vcvs = new ColValueScope(Some(1), Some(10), true, true))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(1), Some(5), false, true),
      vcvs = new ColValueScope(Some(1), Some(10), false, true))
    assert(compensates.isEmpty)

    // QH < VH, VL < QH, VL < QL
    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(2), Some(5), false, true),
      vcvs = new ColValueScope(Some(1), Some(10), false, true))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(2), Some(5), false, true),
      vcvs = new ColValueScope(Some(1), None, false, false))
    assert(compensates.isEmpty)

    compensates = mvu.getCompensateValueScopes(IntegerType,
      qcvs = new ColValueScope(Some(2), Some(5), false, true),
      vcvs = new ColValueScope(None, Some(10), false, true))
    assert(compensates.isEmpty)
  }

  test("testCompensateValue with different data type") {
    val mvu = MaterializedViewUtil
    // double vs double
    assert(mvu.compareLiteral(1.0, 0.1) > 0)
    assert(mvu.compareLiteral(1.0, 1.0) == 0)
    assert(mvu.compareLiteral(1.0, 2.1) < 0)

    // double vs float
    assert(mvu.compareLiteral(1.0, 0.1f) > 0)
    assert(mvu.compareLiteral(1.0, 1.0f) == 0)
    assert(mvu.compareLiteral(1.0, 2.1f) < 0)

    // double vs byte
    assert(mvu.compareLiteral(1.0, 0.toByte) > 0)
    assert(mvu.compareLiteral(1.0, 1.toByte) == 0)
    assert(mvu.compareLiteral(1.0, 2.toByte) < 0)

    // double vs short
    assert(mvu.compareLiteral(1.0, 0.toShort) > 0)
    assert(mvu.compareLiteral(1.0, 1.toShort) == 0)
    assert(mvu.compareLiteral(1.0, 2.toShort) < 0)

    // double vs int
    assert(mvu.compareLiteral(1.0, 0) > 0)
    assert(mvu.compareLiteral(1.0, 1) == 0)
    assert(mvu.compareLiteral(1.0, 2) < 0)

    // double vs long
    intercept[TryMaterializedFailedException] {
      mvu.compareLiteral(1.0, 0L)
    }

    // double vs decimal
    assert(mvu.compareLiteral(1.0, Decimal(0)) > 0)
    assert(mvu.compareLiteral(1.0, Decimal(1)) == 0)
    assert(mvu.compareLiteral(1.0, Decimal(2)) < 0)

    // float vs double
    assert(mvu.compareLiteral(1.0f, 0.1) > 0)
    assert(mvu.compareLiteral(1.0f, 1.0) == 0)
    assert(mvu.compareLiteral(1.0f, 2.1) < 0)

    // float vs float
    assert(mvu.compareLiteral(1.0f, 0.1f) > 0)
    assert(mvu.compareLiteral(1.0f, 1.0f) == 0)
    assert(mvu.compareLiteral(1.0f, 2.1f) < 0)

    // float vs byte
    assert(mvu.compareLiteral(1.0f, 0.toByte) > 0)
    assert(mvu.compareLiteral(1.0f, 1.toByte) == 0)
    assert(mvu.compareLiteral(1.0f, 2.toByte) < 0)

    // float vs short
    assert(mvu.compareLiteral(1.0f, 0.toShort) > 0)
    assert(mvu.compareLiteral(1.0f, 1.toShort) == 0)
    assert(mvu.compareLiteral(1.0f, 2.toShort) < 0)

    // float vs int
    assert(mvu.compareLiteral(1.0f, 0) > 0)
    assert(mvu.compareLiteral(1.0f, 1) == 0)
    assert(mvu.compareLiteral(1.0f, 2) < 0)

    // float vs long
    intercept[TryMaterializedFailedException] {
      mvu.compareLiteral(1.0f, 0L)
    }

    // float vs decimal
    assert(mvu.compareLiteral(1.0f, Decimal(0)) > 0)
    assert(mvu.compareLiteral(1.0f, Decimal(1)) == 0)
    assert(mvu.compareLiteral(1.0f, Decimal(2)) < 0)

    // long vs double
    intercept[TryMaterializedFailedException] {
      mvu.compareLiteral(1L, 0.1)
    }

    // long vs float
    intercept[TryMaterializedFailedException] {
      mvu.compareLiteral(1L, 0.1f)
    }

    // long vs byte
    assert(mvu.compareLiteral(1L, 0.toByte) > 0)
    assert(mvu.compareLiteral(1L, 1.toByte) == 0)
    assert(mvu.compareLiteral(1L, 2.toByte) < 0)

    // long vs short
    assert(mvu.compareLiteral(1L, 0.toShort) > 0)
    assert(mvu.compareLiteral(1L, 1.toShort) == 0)
    assert(mvu.compareLiteral(1L, 2.toShort) < 0)

    // long vs int
    assert(mvu.compareLiteral(1L, 0) > 0)
    assert(mvu.compareLiteral(1L, 1) == 0)
    assert(mvu.compareLiteral(1L, 2) < 0)

    // long vs long
    assert(mvu.compareLiteral(1L, 0L) > 0)
    assert(mvu.compareLiteral(1L, 1L) == 0)
    assert(mvu.compareLiteral(1L, 2L) < 0)

    // long vs decimal
    assert(mvu.compareLiteral(1L, Decimal(0)) > 0)
    assert(mvu.compareLiteral(1L, Decimal(1)) == 0)
    assert(mvu.compareLiteral(1L, Decimal(2)) < 0)

    // int vs double
    assert(mvu.compareLiteral(1, 0.0) > 0)
    assert(mvu.compareLiteral(1, 1.0) == 0)
    assert(mvu.compareLiteral(1, 2.0) < 0)

    // int vs float
    assert(mvu.compareLiteral(1, 0.0f) > 0)
    assert(mvu.compareLiteral(1, 1.0f) == 0)
    assert(mvu.compareLiteral(1, 2.0f) < 0)

    // int vs byte
    assert(mvu.compareLiteral(1, 0.toByte) > 0)
    assert(mvu.compareLiteral(1, 1.toByte) == 0)
    assert(mvu.compareLiteral(1, 2.toByte) < 0)

    // int vs short
    assert(mvu.compareLiteral(1, 0.toShort) > 0)
    assert(mvu.compareLiteral(1, 1.toShort) == 0)
    assert(mvu.compareLiteral(1, 2.toShort) < 0)

    // int vs int
    assert(mvu.compareLiteral(1, 0) > 0)
    assert(mvu.compareLiteral(1, 1) == 0)
    assert(mvu.compareLiteral(1, 2) < 0)

    // int vs long
    assert(mvu.compareLiteral(1, 0L) > 0)
    assert(mvu.compareLiteral(1, 1L) == 0)
    assert(mvu.compareLiteral(1, 2L) < 0)

    // int vs decimal
    assert(mvu.compareLiteral(1, Decimal(0)) > 0)
    assert(mvu.compareLiteral(1, Decimal(1)) == 0)
    assert(mvu.compareLiteral(1, Decimal(2)) < 0)

    // short vs double
    assert(mvu.compareLiteral(1.toShort, 0.0) > 0)
    assert(mvu.compareLiteral(1.toShort, 1.0) == 0)
    assert(mvu.compareLiteral(1.toShort, 2.0) < 0)

    // short vs float
    assert(mvu.compareLiteral(1.toShort, 0.0f) > 0)
    assert(mvu.compareLiteral(1.toShort, 1.0f) == 0)
    assert(mvu.compareLiteral(1.toShort, 2.0f) < 0)

    // short vs byte
    assert(mvu.compareLiteral(1.toShort, 0.toByte) > 0)
    assert(mvu.compareLiteral(1.toShort, 1.toByte) == 0)
    assert(mvu.compareLiteral(1.toShort, 2.toByte) < 0)

    // short vs short
    assert(mvu.compareLiteral(1.toShort, 0.toShort) > 0)
    assert(mvu.compareLiteral(1.toShort, 1.toShort) == 0)
    assert(mvu.compareLiteral(1.toShort, 2.toShort) < 0)

    // short vs int
    assert(mvu.compareLiteral(1.toShort, 0) > 0)
    assert(mvu.compareLiteral(1.toShort, 1) == 0)
    assert(mvu.compareLiteral(1.toShort, 2) < 0)

    // short vs long
    assert(mvu.compareLiteral(1.toShort, 0L) > 0)
    assert(mvu.compareLiteral(1.toShort, 1L) == 0)
    assert(mvu.compareLiteral(1.toShort, 2L) < 0)

    // short vs decimal
    assert(mvu.compareLiteral(1.toShort, Decimal(0)) > 0)
    assert(mvu.compareLiteral(1.toShort, Decimal(1)) == 0)
    assert(mvu.compareLiteral(1.toShort, Decimal(2)) < 0)

    // byte vs double
    assert(mvu.compareLiteral(1.toByte, 0.0) > 0)
    assert(mvu.compareLiteral(1.toByte, 1.0) == 0)
    assert(mvu.compareLiteral(1.toByte, 2.0) < 0)

    // byte vs float
    assert(mvu.compareLiteral(1.toByte, 0.0f) > 0)
    assert(mvu.compareLiteral(1.toByte, 1.0f) == 0)
    assert(mvu.compareLiteral(1.toByte, 2.0f) < 0)

    // byte vs byte
    assert(mvu.compareLiteral(1.toByte, 0.toByte) > 0)
    assert(mvu.compareLiteral(1.toByte, 1.toByte) == 0)
    assert(mvu.compareLiteral(1.toByte, 2.toByte) < 0)

    // byte vs short
    assert(mvu.compareLiteral(1.toByte, 0.toShort) > 0)
    assert(mvu.compareLiteral(1.toByte, 1.toShort) == 0)
    assert(mvu.compareLiteral(1.toByte, 2.toShort) < 0)

    // byte vs int
    assert(mvu.compareLiteral(1.toByte, 0) > 0)
    assert(mvu.compareLiteral(1.toByte, 1) == 0)
    assert(mvu.compareLiteral(1.toByte, 2) < 0)

    // byte vs long
    assert(mvu.compareLiteral(1.toByte, 0L) > 0)
    assert(mvu.compareLiteral(1.toByte, 1L) == 0)
    assert(mvu.compareLiteral(1.toByte, 2L) < 0)

    // byte vs decimal
    assert(mvu.compareLiteral(1.toByte, Decimal(0)) > 0)
    assert(mvu.compareLiteral(1.toByte, Decimal(1)) == 0)
    assert(mvu.compareLiteral(1.toByte, Decimal(2)) < 0)

    // decimal vs double
    assert(mvu.compareLiteral(Decimal(1), 0.0) > 0)
    assert(mvu.compareLiteral(Decimal(1), 1.0) == 0)
    assert(mvu.compareLiteral(Decimal(1), 2.0) < 0)

    // decimal vs float
    assert(mvu.compareLiteral(Decimal(1), 0.0f) > 0)
    assert(mvu.compareLiteral(Decimal(1), 1.0f) == 0)
    assert(mvu.compareLiteral(Decimal(1), 2.0f) < 0)

    // decimal vs byte
    assert(mvu.compareLiteral(Decimal(1), 0.toByte) > 0)
    assert(mvu.compareLiteral(Decimal(1), 1.toByte) == 0)
    assert(mvu.compareLiteral(Decimal(1), 2.toByte) < 0)

    // decimal vs short
    assert(mvu.compareLiteral(Decimal(1), 0.toShort) > 0)
    assert(mvu.compareLiteral(Decimal(1), 1.toShort) == 0)
    assert(mvu.compareLiteral(Decimal(1), 2.toShort) < 0)

    // decimal vs int
    assert(mvu.compareLiteral(Decimal(1), 0) > 0)
    assert(mvu.compareLiteral(Decimal(1), 1) == 0)
    assert(mvu.compareLiteral(Decimal(1), 2) < 0)

    // decimal vs long
    assert(mvu.compareLiteral(Decimal(1), 0L) > 0)
    assert(mvu.compareLiteral(Decimal(1), 1L) == 0)
    assert(mvu.compareLiteral(Decimal(1), 2L) < 0)

    // decimal vs decimal
    assert(mvu.compareLiteral(Decimal(1), Decimal(0)) > 0)
    assert(mvu.compareLiteral(Decimal(1), Decimal(1)) == 0)
    assert(mvu.compareLiteral(Decimal(1), Decimal(2)) < 0)

    // string vs string
    assert(mvu.compareLiteral(CatalystTypeConverters.convertToCatalyst("aa"),
      CatalystTypeConverters.convertToCatalyst("bb")) != 0)
    assert(mvu.compareLiteral(CatalystTypeConverters.convertToCatalyst("aa"),
      CatalystTypeConverters.convertToCatalyst("aa")) == 0)
  }

  // vs1: [L1, H1]
  // vs2: [L2, H2]
  // H1 > H2:
  //    L1 > H2: []
  //    L1 = H2: [] or [L1, L1]
  //    L1 < H2:
  //       L1 > L2: [L1, H2]
  //       L1 = L2: [L1, H2] or [L2, H2]
  //       L1 < L2: [L2, H2]
  // H1 = H2:
  //    L1 > L2: [L1, H2] or [L1, H1]
  //    L1 = L2: [L1, H1] or [L1, H2] or [L2, H1] or [L2, H2]
  //    L1 < L2: [L2, H2] or [L2, H1]
  // H1 < H2:
  //    L2 > H1: []
  //    L2 = H1: [] or [L1, H2]
  //    L2 < H1:
  //       L2 > L1: [L2, H1]
  //       L2 = L1: [L1, H1] or [L2, H1]
  //       L2 < L1: [L1, H1]
  test("testMergeColValueScopes") {
    val mvu = MaterializedViewUtil
    // H1 > H2, L1 > H2
    var merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, true),
      vs2 = new ColValueScope(Some(-10), Some(-1), true, true))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), None, true, true),
      vs2 = new ColValueScope(Some(-10), Some(-1), true, true))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, true),
      vs2 = new ColValueScope(Some(-1), Some(-1), true, true))
    assert(merged.isEmpty)

    // H1 > H2, L1 = H2
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, true),
      vs2 = new ColValueScope(Some(-10), Some(1), true, true))
    var rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 1
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, true),
      vs2 = new ColValueScope(Some(1), Some(1), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 1
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), false, true),
      vs2 = new ColValueScope(Some(-10), Some(1), true, true))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, true),
      vs2 = new ColValueScope(Some(-10), Some(1), true, false))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), false, true),
      vs2 = new ColValueScope(Some(-10), Some(1), true, false))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), false, true),
      vs2 = new ColValueScope(Some(1), Some(1), true, true))
    assert(merged.isEmpty)

    // H1 > H2, L1 < H2, L1 > L2
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(-1), Some(10), true, true),
      vs2 = new ColValueScope(Some(-10), Some(1), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == -1 && rcs.highValue.get == 1
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(-1), None, true, true),
      vs2 = new ColValueScope(None, Some(1), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == -1 && rcs.highValue.get == 1
      && rcs.includeLowValue && rcs.includeHighValue)

    // H1 > H2, L1 < H2, L1 = L2
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(-1), Some(10), true, true),
      vs2 = new ColValueScope(Some(-1), Some(1), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == -1 && rcs.highValue.get == 1
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(-1), Some(10), false, true),
      vs2 = new ColValueScope(Some(-1), Some(1), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == -1 && rcs.highValue.get == 1
      && !rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(-1), Some(10), true, true),
      vs2 = new ColValueScope(Some(-1), Some(1), false, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == -1 && rcs.highValue.get == 1
      && !rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(-1), Some(10), false, true),
      vs2 = new ColValueScope(Some(-1), Some(1), false, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == -1 && rcs.highValue.get == 1
      && !rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(None, Some(10), false, true),
      vs2 = new ColValueScope(None, Some(1), false, true))
    rcs = merged(0)
    assert(rcs.lowValue.isEmpty && rcs.highValue.get == 1
      && !rcs.includeLowValue && rcs.includeHighValue)

    // H1 > H2, L1 < H2, L1 < L2
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(-10), Some(10), false, true),
      vs2 = new ColValueScope(Some(-1), Some(1), false, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == -1 && rcs.highValue.get == 1
      && !rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(None, Some(10), false, true),
      vs2 = new ColValueScope(Some(-1), Some(1), false, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == -1 && rcs.highValue.get == 1
      && !rcs.includeLowValue && rcs.includeHighValue)

    // H1 = H2, L1 > L2
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, true),
      vs2 = new ColValueScope(Some(0), Some(10), false, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(10), Some(10), true, true),
      vs2 = new ColValueScope(Some(0), Some(10), false, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 10 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, false),
      vs2 = new ColValueScope(Some(0), Some(10), false, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && !rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, false),
      vs2 = new ColValueScope(Some(0), Some(10), false, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && !rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, true),
      vs2 = new ColValueScope(Some(0), Some(10), false, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && !rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), None, true, true),
      vs2 = new ColValueScope(Some(0), None, false, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.isEmpty
      && rcs.includeLowValue && !rcs.includeHighValue)

    // H1 = H2, L1 = L2
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, true),
      vs2 = new ColValueScope(Some(1), Some(10), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), false, false),
      vs2 = new ColValueScope(Some(1), Some(10), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && !rcs.includeLowValue && !rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), false, false),
      vs2 = new ColValueScope(Some(1), Some(10), false, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && !rcs.includeLowValue && !rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(10), true, true),
      vs2 = new ColValueScope(Some(1), Some(10), false, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && !rcs.includeLowValue && !rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(1), true, true),
      vs2 = new ColValueScope(Some(1), Some(1), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 1
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(1), true, false),
      vs2 = new ColValueScope(Some(1), Some(1), true, true))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(1), false, true),
      vs2 = new ColValueScope(Some(1), Some(1), true, true))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(1), true, true),
      vs2 = new ColValueScope(Some(1), Some(1), true, false))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(1), Some(1), true, true),
      vs2 = new ColValueScope(Some(1), Some(1), false, true))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(None, None, true, true),
      vs2 = new ColValueScope(None, None, false, true))
    rcs = merged(0)
    assert(rcs.lowValue.isEmpty && rcs.highValue.isEmpty
      && !rcs.includeLowValue && rcs.includeHighValue)

    // H1 = H2, L1 < L2
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, true),
      vs2 = new ColValueScope(Some(1), Some(10), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, true),
      vs2 = new ColValueScope(Some(10), Some(10), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 10 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, false),
      vs2 = new ColValueScope(Some(1), Some(10), true, true))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && !rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, false),
      vs2 = new ColValueScope(Some(1), Some(10), true, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && !rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, true),
      vs2 = new ColValueScope(Some(1), Some(10), true, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.get == 10
      && rcs.includeLowValue && !rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), None, true, true),
      vs2 = new ColValueScope(Some(1), None, true, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 1 && rcs.highValue.isEmpty
      && rcs.includeLowValue && !rcs.includeHighValue)

    // H1 < H2, L2 > H1
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, true),
      vs2 = new ColValueScope(Some(11), Some(100), true, false))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(0), true, true),
      vs2 = new ColValueScope(Some(11), Some(100), true, false))
    assert(merged.isEmpty)

    // H1 < H2, L2 = H1
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, true),
      vs2 = new ColValueScope(Some(10), Some(100), true, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 10 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(10), Some(10), true, true),
      vs2 = new ColValueScope(Some(10), Some(100), true, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 10 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, false),
      vs2 = new ColValueScope(Some(10), Some(100), true, false))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, false),
      vs2 = new ColValueScope(Some(10), Some(100), false, false))
    assert(merged.isEmpty)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, true),
      vs2 = new ColValueScope(Some(10), Some(100), false, false))
    assert(merged.isEmpty)

    // H1 < H2, L2 < H1, L2 > L1
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, true),
      vs2 = new ColValueScope(Some(5), Some(100), true, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 5 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    // H1 < H2, L2 < H1, L2 = L1
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, true),
      vs2 = new ColValueScope(Some(0), Some(100), true, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 0 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), false, true),
      vs2 = new ColValueScope(Some(0), Some(100), true, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 0 && rcs.highValue.get == 10
      && !rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), true, true),
      vs2 = new ColValueScope(Some(0), Some(100), false, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 0 && rcs.highValue.get == 10
      && !rcs.includeLowValue && rcs.includeHighValue)

    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(0), Some(10), false, true),
      vs2 = new ColValueScope(Some(0), Some(100), false, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 0 && rcs.highValue.get == 10
      && !rcs.includeLowValue && rcs.includeHighValue)

    // H1 < H2, L2 < H1, L2 < L1
    merged = mvu.mergeColValueScope(IntegerType,
      vs1 = new ColValueScope(Some(5), Some(10), true, true),
      vs2 = new ColValueScope(Some(0), Some(100), false, false))
    rcs = merged(0)
    assert(rcs.lowValue.get == 5 && rcs.highValue.get == 10
      && rcs.includeLowValue && rcs.includeHighValue)
  }

}
