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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

trait NestNode {

  def ordinal(): Int

  def isRootNode: Boolean

  def isLeafNode: Boolean

  def rootNode(): NestNode

  def leafNode(): Seq[NestNode]

  def parentNode(): NestNode

  def childrenNode(): Seq[NestNode]

  def parentExpression(): Option[Expression]

  def expression(): Expression

  def addChildNode(node: NestNode): Unit

  def cartesian(data: Option[Any]): CartesianData

  def refreshCollect(collect: CartesianData => Unit): Unit

  def dataType(): Option[DataType]
}

abstract class AbstractNestNode(ordinal: Int = -1) extends NestNode {

  val childrenNode_ : ArrayBuffer[NestNode] = new ArrayBuffer[NestNode]()

  val parentNode_ : Option[NestNode] = init()

  var collect: CartesianData => Unit = _

  protected def init(): Option[NestNode] = {
    parentExpression().map(p => {
      val wrapper = NestNodeUtils.wrapper(p)
      wrapper.addChildNode(this)
      wrapper
    })
  }

  override def isRootNode: Boolean = parentNode_.isEmpty

  override def isLeafNode: Boolean = childrenNode().isEmpty

  override def rootNode(): NestNode = {
    var p: NestNode = this
    while (!p.isRootNode) {
      p = p.parentNode()
    }
    p
  }

  override def leafNode(): Seq[NestNode] = {
    if (childrenNode().isEmpty) {
      Seq(this)
    }
    else {
      childrenNode().flatMap(_.leafNode())
    }
  }

  override def parentNode(): NestNode = parentNode_.get

  override def childrenNode(): Seq[NestNode] = childrenNode_

  override def addChildNode(node: NestNode): Unit = childrenNode_ += node


  def filterEmptyData(f: Any => CartesianData): Option[Any] => CartesianData = {
    optionData => optionData.map(realData => f.apply(realData))
      .getOrElse(SingleCartesianData.apply(List.empty, ordinal()))
  }

  def selfCartesian(data: Option[Any]): CartesianData = {
    filterEmptyData(doSelfCartesian)(data)
  }

  def doSelfCartesian(data: Any): CartesianData

  def dataToCartesian(data: Any): CartesianData = {
    data match {
      case array: ArrayData => SingleCartesianData.apply(
        (0 until array.numElements()).map(n =>
          array.get(n, fieldDataType().asInstanceOf[ArrayType].elementType)
        ).toList, ordinal()
      )
      case other => SingleCartesianData.apply(List(other), ordinal())
    }
  }

  def childCartesian(data: Option[Any]): CartesianData = {
    val cartesianData = CartesianData.reduce(childrenNode().map(_.cartesian(data)))

    if (collect != null) {
      collect.apply(cartesianData)
    }

    cartesianData
  }

  override def cartesian(data: Option[Any]): CartesianData = {
    if (isLeafNode) {
      selfCartesian(data)
    }
    else {
      childCartesian(data)
    }
  }

  def fieldOrdinal(): Int

  def fieldDataType(): DataType

  override def refreshCollect(collect: CartesianData => Unit): Unit = {
    this.collect = collect
  }

  override def dataType(): Option[DataType] = {
    if (ordinal() >= 0) {
      val dataType = fieldDataType()
      dataType match {
        case ArrayType(eleType, _) => Some(eleType)
        case _ => Some(dataType)
      }
    }
    else {
      None
    }
  }

  override def hashCode(): Int = expression().references.hashCode()

  override def equals(obj: Any): Boolean = {
    obj match {
      case n: NestNode => expression().equals(n.expression())
      case _ => false
    }
  }
}

abstract class InternalRowExtractNestNode(ordinal: Int) extends AbstractNestNode(ordinal) {

  override def doSelfCartesian(data: Any): CartesianData = {
    val row: InternalRow = data.asInstanceOf[InternalRow]
    if (row.isNullAt(fieldOrdinal())) {
      SingleCartesianData.apply(List.empty, ordinal)
    }
    else {
      val output = row.get(fieldOrdinal(), fieldDataType())
      dataToCartesian(output)
    }
  }

  def outputData(row: InternalRow): Option[Any] = {
    if (row.isNullAt(fieldOrdinal())) {
      None
    } else {
      Some(row.get(fieldOrdinal(), fieldDataType()))
    }
  }

  override def childCartesian(data: Option[Any]): CartesianData = {
    data.map(realData => {
      val output = outputData(realData.asInstanceOf[InternalRow])

      fieldDataType() match {
        case arrayType: ArrayType =>
          output.map(o => {
            val arrayData = o.asInstanceOf[ArrayData]
            if (arrayData.numElements() == 0) {
              super.childCartesian(None)
            }
            else {
              (0 until arrayData.numElements())
                .map(n => super.childCartesian(
                  Some(arrayData.get(n, arrayType.elementType)))
                ).reduce((c1, c2) => c1.append(c2))
            }
          }).getOrElse(super.childCartesian(None))
        case _: StructType => super.childCartesian(output)
      }
    }).getOrElse(super.childCartesian(None))
  }
}

case class GetStructFieldNestNode(gsf: GetStructField, ordinal: Int)
  extends InternalRowExtractNestNode(ordinal) {

  override def fieldOrdinal(): Int = gsf.ordinal

  override def fieldDataType(): DataType =
    gsf.child.dataType.asInstanceOf[StructType](fieldOrdinal()).dataType

  override def parentExpression(): Option[Expression] = Some(gsf.child)

  override def expression(): Expression = gsf
}

case class GetArrayStructFieldsNestNode(gasf: GetArrayStructFields, ordinal: Int)
  extends InternalRowExtractNestNode(ordinal) {

  override def fieldOrdinal(): Int = gasf.ordinal

  override def fieldDataType(): DataType = gasf.field.dataType

  override def parentExpression(): Option[Expression] = Some(gasf.child)

  override def expression(): Expression = gasf
}

case class ExtractNestedArrayFieldNestNode(enaf: ExtractNestedArrayField, ordinal: Int)
  extends InternalRowExtractNestNode(ordinal) {

  override def fieldOrdinal(): Int = enaf.ordinal

  override def fieldDataType(): DataType = enaf.field.dataType

  override def parentExpression(): Option[Expression] = Some(enaf.child)

  override def expression(): Expression = enaf
}

case class BoundReferenceNestNode(br: BoundReference, ordinal: Int)
  extends InternalRowExtractNestNode(ordinal) {

  override def doSelfCartesian(data: Any): CartesianData = {
    val realData = br.eval(data.asInstanceOf[InternalRow])
    SingleCartesianData(tranData(realData, fieldDataType()).toList, ordinal)
  }

  def tranData(data: Any, dataType: DataType): Seq[Any] = {
    if (data == null) {
      Seq.empty
    }
    else {
      data match {
        case array: ArrayData =>
          val elementType: DataType = dataType.asInstanceOf[ArrayType].elementType
          (0 until array.numElements()).flatMap(n => {
            tranData(array.get(n, elementType), elementType)
          })
        case other => Seq(other)
      }
    }
  }

  override def outputData(row: InternalRow): Option[Any] = Some(br.eval(row))

  override def fieldOrdinal(): Int = 0

  override def fieldDataType(): DataType = br.dataType

  override def parentExpression(): Option[Expression] = None

  override def expression(): Expression = br
}

object NestNodeUtils {

  /**
   * Wrapping the received Expression into NestNode
   */
  def wrapper(expression: Expression, original: Int = -1): NestNode = {
    expression match {
      case gasf: GetArrayStructFields => GetArrayStructFieldsNestNode(gasf, original)
      case gsf: GetStructField => GetStructFieldNestNode(gsf, original)
      case enaf: ExtractNestedArrayField => ExtractNestedArrayFieldNestNode(enaf, original)
      case br: BoundReference => BoundReferenceNestNode(br, original)
      case _ =>
        throw new UnsupportedOperationException(s"not support Expression: ${expression.toString}")
    }
  }

  /**
   * Merge all NestNodes, prefixes of the same path will be merged
   */
  def merger(nodes: Seq[NestNode]): Seq[NestNode] = {
    nodes.map(n => ArrayBuffer(n.rootNode())).reduceLeft((nodeForest, singleNode) => {
      nodeForest.find(n => n == singleNode.head)
        .map(n => merger(n, singleNode.head))
        .getOrElse({
          nodeForest.appendAll(singleNode)
          singleNode.head
        })
      nodeForest
    })
  }

  private def merger(aNode: NestNode, bNode: NestNode): NestNode = {
    assert(bNode.childrenNode().size == 1)

    var explodeANode: NestNode = aNode
    var explodeBNode: Option[NestNode] = bNode.childrenNode().headOption
    while (explodeBNode.isDefined) {
      explodeANode.childrenNode()
        .find(n => n == explodeBNode.get)
        .map(n => {
          explodeANode = n
          explodeBNode = explodeBNode.get.childrenNode().headOption
        })
        .getOrElse({
          explodeANode.addChildNode(explodeBNode.get)
          explodeBNode = None
        })
    }
    aNode
  }

  def getCollectNode(node: NestNode, markNode: NestNode): NestNode = {
    var tempNode: Seq[NestNode] = Seq(node.rootNode())
    var tempMarkNode: NestNode = markNode.rootNode()
    var matchNode: Option[NestNode] = tempNode.find(n => n == tempMarkNode)
    while (matchNode.isDefined && !tempMarkNode.isLeafNode) {
      tempNode = matchNode.get.childrenNode()
      tempMarkNode = tempMarkNode.childrenNode().head
      matchNode = tempNode.find(n => n == tempMarkNode)
    }
    matchNode match {
      case n: Some[NestNode] => n.get
      case _ => throw new RuntimeException("not found mark node")
    }
  }


}