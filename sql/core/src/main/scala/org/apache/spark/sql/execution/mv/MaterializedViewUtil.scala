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

import java.math.BigDecimal

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, Not, Or}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.util.Utils

object MaterializedViewUtil {

  val QUERY_LOW_VALUE: Int = 1
  val QUERY_HIGH_VALUE: Int = 2
  val VIEW_LOW_VALUE: Int = 3
  val VIEW_HIGH_VALUE: Int = 4
  val ATT_COMPARE_POSITION_LEFT: String = "left"
  val ATT_COMPARE_POSITION_RIGHT: String = "right"

  // for join situation, query with the condition as t1.c1 = t2.c2
  // it should be filtered if view has this already
  // or and not are not suppoted
  def filterConditions(
      expr: Expression,
      tableAliasMap: Map[String, String],
      tnames: Set[String]): (Boolean, Expression) = {
    var newAnds = Seq.empty[Expression]
    var subExprs = Seq.empty[Expression]

    expr foreach {
      case And(left, right) =>
        if (!left.isInstanceOf[And]) {
          subExprs :+= left
        }
        if (!right.isInstanceOf[And]) {
          subExprs :+= right
        }
      case o: Or =>
        throw new TryMaterializedFailedException("filterConditions: Or expression is unsupported")
      case o: Not =>
        throw new TryMaterializedFailedException("filterConditions: Not expression is unsupported")
      case _ =>
    }

    if (subExprs.isEmpty) {
      expr match {
        case EqualTo(l: AttributeReference, r: AttributeReference) =>
          (isAttInTables(l, tnames, tableAliasMap) ||
            isAttInTables(r, tnames, tableAliasMap), expr)
        case _ => (true, expr)
      }
    } else {
      subExprs foreach {
        case eq@EqualTo(l: AttributeReference, r: AttributeReference) =>
          if (isAttInTables(l, tnames, tableAliasMap) ||
            isAttInTables(r, tnames, tableAliasMap)) {
            newAnds :+= eq
          }
        case ex@_ => newAnds :+= ex
      }

      if (newAnds.isEmpty) {
        (false, expr)
      } else {
        (true, mergeToAnd(newAnds))
      }
    }
  }

  def mergeToAnd(exprs: Seq[Expression]): Expression = {
    if (exprs.size == 1) {
      exprs(0)
    } else {
      var merged = exprs(0)
      for (i <- 1 to exprs.size - 1) {
        merged = And(merged, exprs(i))
      }
      merged
    }
  }

  def mergeColValueScope(
      dataType: DataType,
      vs1: ColValueScope,
      vs2: ColValueScope): Seq[ColValueScope] = {
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
    //    L1 = L2: [L1, H1] or [L1, H2] or [L2, H1] or [L2, H2] or []
    //    L1 < L2: [L2, H2] or [L2, H1]
    // H1 < H2:
    //    L2 > H1: []
    //    L2 = H1: [] or [L1, H2]
    //    L2 < H1:
    //       L2 > L1: [L2, H1]
    //       L2 = L1: [L1, H1] or [L2, H1]
    //       L2 < L1: [L1, H1]
    compareVale(dataType, vs1.highValue, true, vs2.highValue, true) match {
      case r if r > 0 => compareVale(dataType, vs1.lowValue, false, vs2.highValue, true) match {
        case r if r > 0 => Seq.empty[ColValueScope]
        case 0 =>
          if (vs1.includeLowValue && vs2.includeHighValue) {
            Seq(new ColValueScope(vs1.lowValue, vs1.lowValue, true, true))
          } else {
            Seq.empty[ColValueScope]
          }
        case r if r < 0 => compareVale(dataType, vs1.lowValue, false, vs2.lowValue, false) match {
          case r if r > 0 => Seq(new ColValueScope(vs1.lowValue, vs2.highValue,
            vs1.includeLowValue, vs2.includeHighValue))
          case 0 => Seq(new ColValueScope(vs1.lowValue, vs2.highValue,
            vs2.includeLowValue && vs1.includeLowValue, vs2.includeHighValue))
          case r if r < 0 => Seq(vs2)
        }
      }
      case 0 => compareVale(dataType, vs1.lowValue, false, vs2.lowValue, false) match {
        case r if r > 0 => Seq(new ColValueScope(vs1.lowValue, vs1.highValue,
          vs1.includeLowValue, vs1.includeHighValue && vs2.includeHighValue))
        case 0 => compareVale(dataType, vs1.lowValue, false, vs1.highValue, true) match {
          case 0 => if (vs1.includeLowValue && vs1.includeHighValue &&
              vs2.includeLowValue && vs2.includeHighValue) {
            Seq(vs1)
          } else {
            Seq.empty[ColValueScope]
          }
          case _ => Seq(new ColValueScope(vs2.lowValue, vs2.highValue,
            vs1.includeLowValue && vs2.includeLowValue,
            vs1.includeHighValue && vs2.includeHighValue))
        }
        case r if r < 0 => Seq(new ColValueScope(vs2.lowValue, vs2.highValue,
          vs2.includeLowValue, vs1.includeHighValue && vs2.includeHighValue))
      }
      case r if r < 0 => compareVale(dataType, vs2.lowValue, false, vs1.highValue, true) match {
        case r if r > 0 => Seq.empty[ColValueScope]
        case 0 =>
          if (vs2.includeLowValue && vs1.includeHighValue) {
            Seq(new ColValueScope(vs2.lowValue, vs2.lowValue, true, true))
          } else {
            Seq.empty[ColValueScope]
          }
        case r if r < 0 => compareVale(dataType, vs2.lowValue, false, vs1.lowValue, false) match {
          case r if r > 0 => Seq(new ColValueScope(vs2.lowValue, vs1.highValue,
            vs2.includeLowValue, vs1.includeHighValue))
          case 0 => Seq(new ColValueScope(vs1.lowValue, vs1.highValue,
            vs2.includeLowValue && vs1.includeLowValue, vs1.includeHighValue))
          case r if r < 0 => Seq(vs1)
        }
      }
    }
  }

  // view scope: [VL, VH]
  // query scope: [QL, QH]
  def getCompensateValueScopes(
      dataType: DataType,
      qcvs: ColValueScope,
      vcvs: ColValueScope): Seq[ColValueScope] = {
    val (p1, p2, _, p4, equal1, equal2, equal3) = valueScopeSort(dataType, qcvs, vcvs)
    if (p1 == VIEW_LOW_VALUE) {
      if (p4 == QUERY_HIGH_VALUE) {
        if (p2 == VIEW_HIGH_VALUE) {
          // [VL, VH, QL, QH]
          if (equal3) {
            // VH = QL, care about the boundary
            Seq(new ColValueScope(qcvs.lowValue,
              qcvs.highValue, !vcvs.includeHighValue && qcvs.includeLowValue,
              qcvs.includeHighValue))
          } else {
            Seq(qcvs)
          }
        } else {
          // [VL, QL, VH, QH]
          Seq(new ColValueScope(vcvs.highValue,
            qcvs.highValue, !vcvs.includeHighValue,
            qcvs.includeHighValue))
        }
      } else {
        // [VL, QL, QH, VH]
        var rcvs = Seq.empty[ColValueScope]
        if (equal1) {
          if (equal2) {
            compareVale(dataType, qcvs.lowValue, false, qcvs.highValue, true) match {
              case 0 =>
                if (qcvs.includeHighValue && qcvs.includeLowValue
                  && !vcvs.includeLowValue && !vcvs.includeHighValue) {
                  rcvs :+= qcvs
                }
              case _ =>
                if (qcvs.includeHighValue && !vcvs.includeHighValue) {
                  rcvs :+= new ColValueScope(qcvs.highValue, qcvs.highValue, true, true)
                }
                if (qcvs.includeLowValue && !vcvs.includeLowValue) {
                  rcvs :+= new ColValueScope(qcvs.lowValue, qcvs.lowValue, true, true)
                }
            }
          } else {
            if (qcvs.includeLowValue && !vcvs.includeLowValue) {
              rcvs :+= new ColValueScope(qcvs.lowValue, qcvs.lowValue, true, true)
            }
          }
        } else if (equal2) {
          if (qcvs.includeHighValue && !vcvs.includeHighValue) {
            rcvs :+= new ColValueScope(qcvs.highValue, qcvs.highValue, true, true)
          }
        }
        rcvs
      }
    } else {
      if (p4 == QUERY_HIGH_VALUE) {
        // [QL, VL, VH, QH]
        var rcvs = Seq.empty[ColValueScope]
        if (equal1) {
          if (equal2) {
            compareVale(dataType, vcvs.lowValue, false, vcvs.highValue, true) match {
              case 0 =>
                if (qcvs.includeLowValue && qcvs.includeHighValue
                  && !vcvs.includeLowValue && !vcvs.includeHighValue) {
                  // just a point
                  rcvs :+= qcvs
                }
              case _ =>
                if (qcvs.includeHighValue && !vcvs.includeHighValue) {
                  rcvs :+= new ColValueScope(qcvs.highValue, qcvs.highValue, true, true)
                }
                if (qcvs.includeLowValue && !vcvs.includeLowValue) {
                  rcvs :+= new ColValueScope(qcvs.lowValue, qcvs.lowValue, true, true)
                }
            }
          } else {
            rcvs :+= new ColValueScope(vcvs.highValue, qcvs.highValue,
              !vcvs.includeHighValue, qcvs.includeHighValue)
            if (qcvs.includeLowValue && !vcvs.includeLowValue) {
              rcvs :+= new ColValueScope(qcvs.lowValue, qcvs.lowValue, true, true)
            }
          }
          rcvs
        } else if (equal2) {
          var rcvs = Seq(new ColValueScope(qcvs.lowValue, vcvs.lowValue,
            qcvs.includeLowValue, !vcvs.includeLowValue))
          if (qcvs.includeHighValue && !vcvs.includeHighValue) {
            rcvs :+= new ColValueScope(qcvs.highValue, qcvs.highValue, true, true)
          }
          rcvs
        } else {
          Seq(new ColValueScope(qcvs.lowValue,
            vcvs.lowValue, qcvs.includeLowValue,
            !vcvs.includeLowValue),
            new ColValueScope(vcvs.highValue,
              qcvs.highValue, !vcvs.includeHighValue,
              qcvs.includeHighValue)
          )
        }
      } else {
        if (p2 == QUERY_HIGH_VALUE) {
          // [QL, QH, VL, VH]
          if (equal3) {
            Seq(new ColValueScope(qcvs.lowValue,
              qcvs.highValue, qcvs.includeLowValue,
              qcvs.includeHighValue && !vcvs.includeLowValue))
          } else {
            Seq(qcvs)
          }
        } else {
          // [QL, VL, QH, VH]
          if (equal1) {
            if (qcvs.includeLowValue && !vcvs.includeLowValue) {
              Seq(new ColValueScope(qcvs.lowValue, qcvs.lowValue, true, true))
            } else {
              Seq.empty[ColValueScope]
            }
          } else {
            Seq(new ColValueScope(qcvs.lowValue,
              vcvs.lowValue, qcvs.includeLowValue,
              !vcvs.includeLowValue))
          }
        }
      }
    }
  }

  private def valueScopeSort(
      dataType: DataType,
      queryColValueScope: ColValueScope,
      viewColValueScope: ColValueScope): (Int, Int, Int, Int, Boolean, Boolean, Boolean) = {
    var lowCandidatePos: Int = VIEW_LOW_VALUE
    var lowCandidateValue: Option[Any] = viewColValueScope.lowValue
    var isLowCandidateInclude: Boolean = viewColValueScope.includeLowValue
    var highCandidatePos: Int = VIEW_HIGH_VALUE
    var highCandidateValue: Option[Any] = viewColValueScope.highValue
    var isHighCandidateInclude: Boolean = viewColValueScope.includeHighValue
    var p1 = 0
    var p2 = 0
    var p3 = 0
    var p4 = 0
    var equal1 = false
    var equal2 = false
    var equal3 = false

    val compareLL = compareVale(
      dataType, queryColValueScope.lowValue, false, viewColValueScope.lowValue, false)
    if (compareLL == 0) {
      equal1 = true
      p1 = QUERY_LOW_VALUE
    } else if (compareLL > 0) {
      p1 = VIEW_LOW_VALUE
      lowCandidatePos = QUERY_LOW_VALUE
      lowCandidateValue = queryColValueScope.lowValue
      isLowCandidateInclude = queryColValueScope.includeLowValue
    } else {
      p1 = QUERY_LOW_VALUE
    }

    val compareHH = compareVale(
      dataType, queryColValueScope.highValue, true, viewColValueScope.highValue, true)
    if (compareHH == 0) {
      equal2 = true
      if (p1 == QUERY_LOW_VALUE) {
        p4 = QUERY_HIGH_VALUE
      } else {
        p4 = VIEW_HIGH_VALUE
        highCandidatePos = QUERY_HIGH_VALUE
        highCandidateValue = queryColValueScope.highValue
        isHighCandidateInclude = queryColValueScope.includeHighValue
      }
    } else if (compareHH > 0) {
      p4 = QUERY_HIGH_VALUE
    } else {
      p4 = VIEW_HIGH_VALUE
      highCandidatePos = QUERY_HIGH_VALUE
      highCandidateValue = queryColValueScope.highValue
      isHighCandidateInclude = queryColValueScope.includeHighValue
    }

    val compareLH = compareVale(
      dataType, lowCandidateValue, false, highCandidateValue, true)
    if (compareLH == 0) {
      equal3 = true
      if (lowCandidatePos == VIEW_LOW_VALUE && highCandidatePos == QUERY_HIGH_VALUE) {
        p2 = QUERY_HIGH_VALUE
        p3 = VIEW_LOW_VALUE
      } else if (lowCandidatePos == QUERY_LOW_VALUE && highCandidatePos == VIEW_HIGH_VALUE) {
        p2 = VIEW_HIGH_VALUE
        p3 = QUERY_LOW_VALUE
      } else {
        // it should not be here
        p2 = highCandidatePos
        p3 = lowCandidatePos
      }
    } else if (compareLH > 0) {
      p2 = highCandidatePos
      p3 = lowCandidatePos
    } else {
      p2 = lowCandidatePos
      p3 = highCandidatePos
    }

    (p1, p2, p3, p4, equal1, equal2, equal3)
  }

  private def compareVale(
      dataType: DataType,
      v1: Option[Any],
      isV1High: Boolean,
      v2: Option[Any],
      isV2High: Boolean): Int = {
    if (v1.isEmpty) {
      if (v2.isEmpty) {
        if (isV1High == isV2High) {
          0
        } else if (isV1High && !isV2High) {
          1
        } else {
          -1
        }
      } else {
        if (isV1High) {
          1
        } else {
          -1
        }
      }
    } else {
      if (v2.isEmpty) {
        if (isV2High) {
          -1
        } else {
          1
        }
      } else {
        compareLiteral(v1.get, v2.get)
      }
    }
  }

  private[execution] def compareLiteral(srcValue: Any, dstValue: Any): Int = {
    val (dt, src, dst) = unifyDataType(srcValue, dstValue)
    compareLiteralInternal(dt, src, dst)
  }

  // refer to BinaryComparison.doGenCode
  private def compareLiteralInternal(
      dataType: AbstractDataType, srcValue: Any, dstValue: Any): Int = {
    dataType match {
      case DoubleType =>
        Ordering.Double.compare(srcValue.asInstanceOf[DoubleType.InternalType],
          dstValue.asInstanceOf[DoubleType.InternalType])
      case FloatType =>
        Ordering.Float.compare(srcValue.asInstanceOf[FloatType.InternalType],
          dstValue.asInstanceOf[FloatType.InternalType])
      // use c1 - c2 may overflow
      case ByteType =>
        val byteSrc = srcValue.asInstanceOf[ByteType.InternalType]
        val byteDst = dstValue.asInstanceOf[ByteType.InternalType]
        if (byteSrc > byteDst) {
          1
        } else if (byteSrc == byteDst) {
          0
        } else {
          -1
        }
      case ShortType =>
        val shortSrc = srcValue.asInstanceOf[ShortType.InternalType]
        val shortDst = dstValue.asInstanceOf[ShortType.InternalType]
        if (shortSrc > shortDst) {
          1
        } else if (shortSrc == shortDst) {
          0
        } else {
          -1
        }
      case IntegerType | DateType =>
        val intSrc = srcValue.asInstanceOf[IntegerType.InternalType]
        val intDst = dstValue.asInstanceOf[IntegerType.InternalType]
        if (intSrc > intDst) {
          1
        } else if (intSrc == intDst) {
          0
        } else {
          -1
        }
      case LongType | TimestampType =>
        val longSrc = srcValue.asInstanceOf[LongType.InternalType]
        val longDst = dstValue.asInstanceOf[LongType.InternalType]
        if (longSrc > longDst) {
          1
        } else if (longSrc == longDst) {
          0
        } else {
          -1
        }
      case BinaryType =>
        TypeUtils.compareBinary(srcValue.asInstanceOf[BinaryType.InternalType],
          dstValue.asInstanceOf[BinaryType.InternalType])
      case DecimalType.Fixed(_, _) =>
        val decimalSrc = srcValue.asInstanceOf[Decimal].toBigDecimal
        val decimalDst = dstValue.asInstanceOf[Decimal].toBigDecimal
        decimalSrc.compare(decimalDst)
      case StringType =>
        srcValue.asInstanceOf[StringType.InternalType].compare(
          dstValue.asInstanceOf[StringType.InternalType])
      case _ =>
        throw new TryMaterializedFailedException(s"compareLiteral: doesn't support $dataType")
    }
  }

  private def unifyDataType(srcValue: Any, dstValue: Any): (AbstractDataType, Any, Any) = {
    val PRECISION = 10
    val SCALE = 2

    srcValue match {
      case t: DoubleType.InternalType =>
        getDataType(dstValue) match {
          case DoubleType =>
            (DoubleType, srcValue, dstValue)
          case FloatType =>
            (DecimalType(PRECISION, SCALE),
              Decimal(BigDecimal.valueOf(
                srcValue.asInstanceOf[DoubleType.InternalType]), PRECISION, SCALE),
              Decimal(BigDecimal.valueOf(
                dstValue.asInstanceOf[FloatType.InternalType]), PRECISION, SCALE))
          case ByteType =>
            (DoubleType, srcValue, dstValue.asInstanceOf[ByteType.InternalType].toDouble)
          case ShortType =>
            (DoubleType, srcValue, dstValue.asInstanceOf[ShortType.InternalType].toDouble)
          case IntegerType =>
            (DoubleType, srcValue, dstValue.asInstanceOf[IntegerType.InternalType].toDouble)
          case LongType =>
            throw new TryMaterializedFailedException("Unsupported type compare: [double, long]")
          case DecimalType.Fixed(_, _) =>
            val dstDecimal = dstValue.asInstanceOf[Decimal]
            (DecimalType.forType(DoubleType), Decimal(
              BigDecimal.valueOf(srcValue.asInstanceOf[DoubleType.InternalType]),
              dstDecimal.precision, dstDecimal.scale), dstValue)
          case _ => throw new TryMaterializedFailedException("Unsupported type trans with double")
        }
      case t: FloatType.InternalType =>
        getDataType(dstValue) match {
          case DoubleType =>
            (DoubleType, srcValue.asInstanceOf[FloatType.InternalType].toDouble, dstValue)
          case FloatType =>
            (FloatType, srcValue, dstValue)
          case ByteType =>
            (FloatType, srcValue, dstValue.asInstanceOf[ByteType.InternalType].toFloat)
          case ShortType =>
            (FloatType, srcValue, dstValue.asInstanceOf[ShortType.InternalType].toFloat)
          case IntegerType =>
            (FloatType, srcValue, dstValue.asInstanceOf[IntegerType.InternalType].toFloat)
          case LongType =>
            throw new TryMaterializedFailedException("Unsupported type compare: [float, long]")
          case DecimalType.Fixed(_, _) =>
            val dstDecimal = dstValue.asInstanceOf[Decimal]
            (DecimalType.forType(FloatType), Decimal(
              BigDecimal.valueOf(srcValue.asInstanceOf[FloatType.InternalType]),
              dstDecimal.precision, dstDecimal.scale), dstValue)
          case _ => throw new TryMaterializedFailedException("Unsupported type trans with float")
        }
      case t: LongType.InternalType =>
        getDataType(dstValue) match {
          case DoubleType =>
            throw new TryMaterializedFailedException("Unsupported type compare: [long, double]")
          case FloatType =>
            throw new TryMaterializedFailedException("Unsupported type compare: [long, float]")
          case ByteType =>
            (LongType, srcValue, dstValue.asInstanceOf[ByteType.InternalType].toLong)
          case ShortType =>
            (LongType, srcValue, dstValue.asInstanceOf[ShortType.InternalType].toLong)
          case IntegerType =>
            (LongType, srcValue, dstValue.asInstanceOf[IntegerType.InternalType].toLong)
          case LongType =>
            (LongType, srcValue, dstValue)
          case DecimalType.Fixed(_, _) =>
            val dstDecimal = dstValue.asInstanceOf[Decimal]
            (DecimalType.forType(LongType), Decimal(
              BigDecimal.valueOf(srcValue.asInstanceOf[LongType.InternalType]),
              dstDecimal.precision, dstDecimal.scale), dstValue)
          case _ => throw new TryMaterializedFailedException("Unsupported type trans with long")
        }
      case t: IntegerType.InternalType =>
        getDataType(dstValue) match {
          case DoubleType =>
            (DoubleType, srcValue.asInstanceOf[IntegerType.InternalType].toDouble, dstValue)
          case FloatType =>
            (FloatType, srcValue.asInstanceOf[IntegerType.InternalType].toFloat, dstValue)
          case ByteType =>
            (IntegerType, srcValue, dstValue.asInstanceOf[ByteType.InternalType].toInt)
          case ShortType =>
            (IntegerType, srcValue, dstValue.asInstanceOf[ShortType.InternalType].toInt)
          case IntegerType =>
            (IntegerType, srcValue, dstValue)
          case LongType =>
            (LongType, srcValue.asInstanceOf[IntegerType.InternalType].toLong, dstValue)
          case DecimalType.Fixed(_, _) =>
            val dstDecimal = dstValue.asInstanceOf[Decimal]
            (DecimalType.forType(IntegerType), Decimal(
              BigDecimal.valueOf(srcValue.asInstanceOf[IntegerType.InternalType]),
              dstDecimal.precision, dstDecimal.scale), dstValue)
          case _ => throw new TryMaterializedFailedException("Unsupported type trans with int")
        }
      case t: ShortType.InternalType =>
        getDataType(dstValue) match {
          case DoubleType =>
            (DoubleType, srcValue.asInstanceOf[ShortType.InternalType].toDouble, dstValue)
          case FloatType =>
            (FloatType, srcValue.asInstanceOf[ShortType.InternalType].toFloat, dstValue)
          case ByteType =>
            (ShortType, srcValue, dstValue.asInstanceOf[ByteType.InternalType].toShort)
          case ShortType =>
            (ShortType, srcValue, dstValue)
          case IntegerType =>
            (IntegerType, srcValue.asInstanceOf[ShortType.InternalType].toInt, dstValue)
          case LongType =>
            (LongType, srcValue.asInstanceOf[ShortType.InternalType].toLong, dstValue)
          case DecimalType.Fixed(_, _) =>
            val dstDecimal = dstValue.asInstanceOf[Decimal]
            (DecimalType.forType(ShortType), Decimal(
              BigDecimal.valueOf(srcValue.asInstanceOf[ShortType.InternalType]),
              dstDecimal.precision, dstDecimal.scale), dstValue)
          case _ => throw new TryMaterializedFailedException("Unsupported type trans with int")
        }
      case t: ByteType.InternalType =>
        getDataType(dstValue) match {
          case DoubleType =>
            (DoubleType, srcValue.asInstanceOf[ByteType.InternalType].toDouble, dstValue)
          case FloatType =>
            (FloatType, srcValue.asInstanceOf[ByteType.InternalType].toFloat, dstValue)
          case ByteType =>
            (ByteType, srcValue, dstValue)
          case ShortType =>
            (ShortType, srcValue.asInstanceOf[ByteType.InternalType].toShort, dstValue)
          case IntegerType =>
            (IntegerType, srcValue.asInstanceOf[ByteType.InternalType].toInt, dstValue)
          case LongType =>
            (LongType, srcValue.asInstanceOf[ByteType.InternalType].toLong, dstValue)
          case DecimalType.Fixed(_, _) =>
            val dstDecimal = dstValue.asInstanceOf[Decimal]
            (DecimalType.forType(ByteType), Decimal(
              BigDecimal.valueOf(srcValue.asInstanceOf[ByteType.InternalType]),
              dstDecimal.precision, dstDecimal.scale), dstValue)
          case _ => throw new TryMaterializedFailedException("Unsupported type trans with int")
        }
      case t: Decimal =>
        val srcDecimal = srcValue.asInstanceOf[Decimal]
        getDataType(dstValue) match {
          case DoubleType =>
            (DecimalType.forType(DoubleType), srcValue, Decimal(
              BigDecimal.valueOf(dstValue.asInstanceOf[DoubleType.InternalType]),
              srcDecimal.precision, srcDecimal.scale))
          case FloatType =>
            (DecimalType.forType(FloatType), srcValue, Decimal(
              BigDecimal.valueOf(dstValue.asInstanceOf[FloatType.InternalType]),
              srcDecimal.precision, srcDecimal.scale))
          case ByteType =>
            (DecimalType.forType(ByteType), srcValue, Decimal(
              BigDecimal.valueOf(dstValue.asInstanceOf[ByteType.InternalType]),
              srcDecimal.precision, srcDecimal.scale))
          case ShortType =>
            (DecimalType.forType(ShortType), srcValue, Decimal(
              BigDecimal.valueOf(dstValue.asInstanceOf[ShortType.InternalType]),
              srcDecimal.precision, srcDecimal.scale))
          case IntegerType =>
            (DecimalType.forType(IntegerType), srcValue, Decimal(
              BigDecimal.valueOf(dstValue.asInstanceOf[IntegerType.InternalType]),
              srcDecimal.precision, srcDecimal.scale))
          case LongType =>
            (DecimalType.forType(LongType), srcValue, Decimal(
              BigDecimal.valueOf(dstValue.asInstanceOf[LongType.InternalType]),
              srcDecimal.precision, srcDecimal.scale))
          case DecimalType.Fixed(_, _) =>
            (DecimalType.forType(DoubleType), srcValue, dstValue)
          case _ => throw new TryMaterializedFailedException("Unsupported type trans with decimal")
        }
      case t: BinaryType.InternalType =>
        getDataType(dstValue) match {
          case BinaryType => (BinaryType, srcValue, dstValue)
          case _ => throw new TryMaterializedFailedException("Unsupported type trans with binary")
        }
      case t: StringType.InternalType =>
        getDataType(dstValue) match {
          case StringType => (StringType, srcValue, dstValue)
          case _ => throw new TryMaterializedFailedException("Unsupported type trans with string")
        }
      case _ =>
        throw new TryMaterializedFailedException("Unsupported type trans when unifyDataType")
    }
  }

  private def getDataType(value: Any): AbstractDataType = {
    value match {
      case v: DoubleType.InternalType => DoubleType
      case v: FloatType.InternalType => FloatType
      case v: ByteType.InternalType => ByteType
      case v: ShortType.InternalType => ShortType
      case v: IntegerType.InternalType => IntegerType
      case v: LongType.InternalType => LongType
      case v: BinaryType.InternalType => BinaryType
      case v: StringType.InternalType => StringType
      case v: Decimal => DecimalType.forType(IntegerType)
      case _ => throw new TryMaterializedFailedException(s"Unsupported data type in mv: $value")
    }
  }

  private def isAttInTables(
      attRef: AttributeReference,
      tableNames: Set[String],
      tableAliasMap: Map[String, String]): Boolean = {
    tableNames.contains(tableAliasMap.getOrElse(attRef.qualifier.mkString("."), ""))
  }
}

class ColValueScope(
  val lowValue: Option[Any],
  val highValue: Option[Any],
  val includeLowValue: Boolean,
  val includeHighValue: Boolean)
