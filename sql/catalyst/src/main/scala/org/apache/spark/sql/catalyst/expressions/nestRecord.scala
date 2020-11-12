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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType}

abstract class RecordConditionExpression(child: Expression, symbol: Expression, value: Expression)
  extends Expression with Serializable {

  override def nullable: Boolean = true

  lazy val nestNode: NestNode = {
    NestNodeUtils.wrapper(child, 0).rootNode()
  }

  lazy val childDataType: Option[DataType] = nestNode.leafNode().head.dataType()

  lazy val defaultValue: Boolean = initDefaultValue()

  lazy val compareFunction: Any => Boolean =
    RecordCompare.getCompareFunction(
      symbol,
      childDataType.map(cType => Cast(value, cType)).getOrElse(value)
    )

  def initDefaultValue(): Boolean

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = Seq(child, symbol, value)

}

/**
 * Determine whether the values of each base type within the nested data are eligible.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Determine whether the values of each base type within " +
    "the nested data are eligible.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(array(struct(array(1, 2)), struct(array(3))), '>', 2);
       false
  """)
case class RecordEvery(child: Expression, symbol: Expression, value: Expression)
  extends RecordConditionExpression(child, symbol, value) with CodegenFallback {

  override def eval(input: InternalRow): Boolean = {
    val singleCartesianData: SingleCartesianData = nestNode.cartesian(Some(input))
      .asInstanceOf[SingleCartesianData]

    if (!singleCartesianData.volatileData()) {
      defaultValue
    }
    else {
      singleCartesianData.data.forall(p => compareFunction.apply(p))
    }
  }

  override def initDefaultValue(): Boolean = {
    symbol match {
      case Literal(s, StringType) => s.toString match {
        case "=" | ">" | ">=" | "<" | "<=" => false
        case "!=" | "<>" => true
        case str => throw new IllegalArgumentException("not support arithmetic symbol" + str)
      }
      case _ => throw new IllegalArgumentException(s"not support arithmetic symbol type " +
        s"${symbol.dataType}")
    }
  }
}

/**
 * Determine if there are eligible values within the nested data.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Determine if there are eligible values within the nested data.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(array(struct(array(1, 2)), struct(array(3))), '>', 2);
       true
  """)
case class RecordSome(child: Expression, symbol: Expression, value: Expression)
  extends RecordConditionExpression(child, symbol, value) with CodegenFallback {

  override def eval(input: InternalRow): Boolean = {
    val singleCartesianData: SingleCartesianData = nestNode.cartesian(Some(input))
      .asInstanceOf[SingleCartesianData]

    if (!singleCartesianData.volatileData()) {
      defaultValue
    }
    else {
      singleCartesianData.data.exists(p => compareFunction.apply(p))
    }
  }

  override def initDefaultValue(): Boolean = {
    symbol match {
      case Literal(s, StringType) => s.toString match {
        case "=" | ">" | ">=" | "<" | "<=" | "!=" | "<>" => false
        case str => throw new IllegalArgumentException("not support arithmetic symbol" + str)
      }
      case _ => throw new IllegalArgumentException(s"not support arithmetic symbol type " +
        s"${symbol.dataType}")
    }
  }
}

/**
 * Compute the count of values within nested data nodes.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Counting the internal values of nested data.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(array(struct(array(1, 2)), struct(array(3))));
       3
  """)
case class RecordCount(child: Expression) extends Expression
  with Serializable with CodegenFallback {

  lazy val nestNode: NestNode = {
    NestNodeUtils.wrapper(child, 0).rootNode()
  }

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Int = {
    Some(nestNode.cartesian(Some(input)))
      .filter(c => c.volatileData())
      .map(c => c.size())
      .getOrElse(0)
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = Seq(child)
}

/**
 * Compute the count of values within nested data nodes.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Sum the internal values of nested data.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(array(struct(array(1, 2)), struct(array(3))));
       6.0
  """)
case class RecordSum(child: Expression) extends Expression
  with Serializable with CodegenFallback {

  lazy val nestNode: NestNode = {
    NestNodeUtils.wrapper(child, 0).rootNode()
  }

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Double = {
    Some(nestNode.cartesian(Some(input)))
      .filter(c => c.volatileData())
      .map(c => c.asInstanceOf[SingleCartesianData])
      .map(c => c.data.filter(d => d != null)
        .map(d => extractDouble(d).getOrElse(0.0)).sum)
      .getOrElse(0.0)
  }

  def extractDouble(x: Any): Option[Double] = x match {
    case n: java.lang.Number => Some(n.doubleValue())
    case _ => None
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = Seq(child)
}


object RecordCompare {

  def getCompareFunction(symbol: Expression, value: Expression): Any => Boolean = {
    val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(value.dataType)
    val vEval = value.eval()
    symbol match {
      case Literal(s, StringType) => s.toString match {
        case "=" => v => compareWithNull(v, v => ordering.equiv(v, vEval))
        case ">" => v => compareWithNull(v, v => ordering.gt(v, vEval))
        case ">=" => v => compareWithNull(v, v => ordering.gteq(v, vEval))
        case "<" => v => compareWithNull(v, v => ordering.lt(v, vEval))
        case "<=" => v => compareWithNull(v, v => ordering.lteq(v, vEval))
        case "!=" | "<>" => v => if (v == null && vEval == null) false
          else if (v == null || vEval == null) true
          else !ordering.equiv(v, vEval)
        case str => throw new IllegalArgumentException("not support arithmetic symbol" + str)
      }
      case _ => throw new IllegalArgumentException(s"not support arithmetic symbol type " +
        s"${symbol.dataType}")
    }
  }

  private def compareWithNull(v: Any, compare: Any => Boolean): Boolean = {
    v != null && compare.apply(v)
  }
}
