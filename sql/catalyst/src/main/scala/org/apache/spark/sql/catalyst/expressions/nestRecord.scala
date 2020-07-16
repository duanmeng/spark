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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, StringType}

abstract class RecordConditionExpression(child: Expression, symbol: Expression, value: Expression)
  extends Expression with Serializable {

  override def nullable: Boolean = true

  lazy val nestNode: NestNode = {
    NestNodeUtils.checkExpression(Seq(child))
    NestNodeUtils.wrapper(child, 0).rootNode()
  }

  lazy val childDataType: Option[DataType] = nestNode.leafNode().head.getDataType()

  lazy val compareFunction: Any => Boolean =
    RecordCompare.getCompareFunction(
      symbol,
      childDataType.map(cType => Cast(value, cType)).getOrElse(value)
    )

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
    val data: List[Any] = nestNode.cartesian(input).asInstanceOf[SingleCartesianData].data

    if (data.isEmpty) {
      false
    }
    else {
      data.forall(p => compareFunction.apply(p))
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
    val data: List[Any] = nestNode.cartesian(input).asInstanceOf[SingleCartesianData].data

    if (data.isEmpty) {
      false
    }
    else {
      data.exists(p => compareFunction.apply(p))
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
    NestNodeUtils.checkExpression(Seq(child))
    NestNodeUtils.wrapper(child, 0).rootNode()
  }

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Int = {
    nestNode.cartesian(input).size()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw new UnsupportedOperationException("not support gen code")
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = Seq(child)
}


object RecordCompare {

  def getCompareFunction(symbol: Expression, value: Expression): Any => Boolean = {
    val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(value.dataType)
    val vEval = value.eval()
    symbol match {
      case Literal(s, StringType) => s.toString match {
        case "=" => v => ordering.equiv(v, vEval)
        case ">" => v => ordering.gt(v, vEval)
        case ">=" => v => ordering.gteq(v, vEval)
        case "<" => v => ordering.lt(v, vEval)
        case "<=" => v => ordering.lteq(v, vEval)
        case "!=" | "<>" => v => !ordering.equiv(v, vEval)
        case _ => _ => false
      }
      case _ => _ => false
    }
  }
}
