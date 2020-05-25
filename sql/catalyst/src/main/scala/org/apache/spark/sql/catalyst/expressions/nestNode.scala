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


/**
 * Nested data, abstraction of each layer of nodes
 */
trait NestNode {

  def ordinal(): Int

  def isRootNode: Boolean

  def firstNodeName(): String

  def selfName(): String

  def fullFieldName(): String

  def prefixFieldName(): String

  def rootNode(): NestNode

  def leafNode(): Seq[NestNode]

  def parentNode(): NestNode

  def childrenNode(): Seq[NestNode]

  def addChildNode(getFieldNode: NestNode): Unit

  def cartesian(internalRow: InternalRow): CartesianData

  def getDataType(): Option[DataType]

  def refreshCollect(collect: CartesianData => Unit): Unit

}

object NestNodeUtils {

  /**
   * Wrapping the received ExtractValue into NestNode
   */
  def wrapper(fields: Any, original: Int = -1): NestNode = {
    fields match {
      case f: GetArrayStructFields => GetArrayStructFieldsNestNode(f, original)
      case f: GetStructField => GetStructFieldsNestNode(f, original)
      case b: BoundReference => BoundReferenceNestNode(b, original)
      case _ =>
        throw new UnsupportedOperationException("not support ExtractValue Expression")
    }
  }

  /**
   * Merge all NestNodes, prefixes of the same path will be merged
   */
  def merger(nodes: Seq[NestNode]): Seq[NestNode] = {
    nodes.map(n => ArrayBuffer(n.rootNode())).reduceLeft((nodeForest, singleNode) => {
      nodeForest.find(n => rootEqual(n, singleNode.head))
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
      aNode.childrenNode()
        .find(n => n.selfName() == explodeBNode.get.selfName())
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

  private def rootEqual(aNode: NestNode, bNode: NestNode): Boolean = {
    assert(aNode.isInstanceOf[BoundReferenceNestNode] && bNode.isInstanceOf[BoundReferenceNestNode])

    aNode.asInstanceOf[BoundReferenceNestNode].canEqual(bNode)
  }

  def getCollectNode(node: NestNode, markNode: NestNode): NestNode = {
    val nodeFullName = node.leafNode().head.fullFieldName()
    val markFullName = markNode.leafNode().head.fullFieldName()
    val prefixMatch = nodeFullName.startsWith(markFullName)
    var tempNode = node
    if (prefixMatch && rootEqual(node, markNode)) {
      for (_ <- 0 until markFullName.toCharArray.count(c => c == '.')) {
        tempNode = tempNode.childrenNode().head
      }
      tempNode
    }
    else {
      throw new IllegalArgumentException(s"$nodeFullName not in $markFullName")
    }
  }

  def checkExpression(child: Seq[Expression]): Unit = {
    child.foreach(e => checkChildExpression(e))
  }

  @scala.annotation.tailrec
  private def checkChildExpression(child: Expression): Unit = {
    child match {
      case GetArrayStructFields(extractValueChild, _, _, _, _) =>
        checkChildExpression(extractValueChild)
      case GetStructField(extractValueChild, _, name) =>
        checkFieldName(name)
        checkChildExpression(extractValueChild)
      case BoundReference(_, _, _) =>
      case _ => throw new UnsupportedOperationException("not support ExtractValue Expression")
    }
  }

  private def checkFieldName(name: Option[String]): Unit = {
    if (name.isEmpty) throw new UnsupportedOperationException("not set field name")
  }
}

abstract class AbstractNestNode extends NestNode {

  val childNode: ArrayBuffer[NestNode] = new ArrayBuffer[NestNode]()

  val _parentNode: Option[NestNode] = init()

  var collect: CartesianData => Unit = _

  protected def getParent: Option[Any]

  protected def init(): Option[NestNode] = {
    getParent.map(p => {
      val wrapper = NestNodeUtils.wrapper(p)
      wrapper.addChildNode(this)
      wrapper
    })
  }

  override def isRootNode: Boolean = false

  override def firstNodeName(): String = {
    if (parentNode().isRootNode) selfName() else parentNode().firstNodeName()
  }

  override def fullFieldName(): String = s"${prefixFieldName()}.${selfName()}"

  override def prefixFieldName(): String = parentNode().fullFieldName()

  override def rootNode(): NestNode = {
    var p: NestNode = this
    while (!p.isRootNode) {
      p = p.parentNode()
    }
    p
  }

  override def leafNode(): Seq[NestNode] = {
    if (childNode.isEmpty) {
      Seq(this)
    }
    else {
      childNode.flatMap(_.leafNode())
    }
  }

  override def parentNode(): NestNode = _parentNode.get

  override def childrenNode(): Seq[NestNode] = childNode

  override def addChildNode(getFieldNode: NestNode): Unit = childNode += getFieldNode

  def childCartesian(internalRow: InternalRow): CartesianData = {
    val data = CartesianData.reduce(childrenNode().map(_.cartesian(internalRow)))

    if (collect != null) {
      collect.apply(data)
    }

    data
  }

  def selfCartesian(internalRow: InternalRow): CartesianData

  override def cartesian(internalRow: InternalRow): CartesianData = {
    if (ordinal() >= 0) {
      selfCartesian(internalRow)
    }
    else {
      childCartesian(internalRow)
    }
  }

  override def getDataType(): Option[DataType] = {
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

  override def refreshCollect(collect: CartesianData => Unit): Unit = {
    this.collect = collect
  }

  def fieldDataType(): DataType

}

abstract class GetFieldsNestNode extends AbstractNestNode {

  def fieldOrdinal(): Int

  override def childCartesian(internalRow: InternalRow): CartesianData = {
    val dataType = fieldDataType()
    val output = internalRow.get(fieldOrdinal(), dataType)
    dataType match {
      case arrayType: ArrayType =>
        val arrayData = output.asInstanceOf[ArrayData]
        (0 until arrayData.numElements())
          .map(n => super.childCartesian(
            arrayData.get(n, arrayType.elementType).asInstanceOf[InternalRow])
          ).reduce((c1, c2) => c1.append(c2))
      case _: StructType => super.childCartesian(output.asInstanceOf[InternalRow])
    }
  }

  override def selfCartesian(internalRow: InternalRow): CartesianData = {
    val dataType = fieldDataType()
    if (internalRow.isNullAt(fieldOrdinal())) {
      SingleCartesianData.apply(List(null), ordinal())
    }
    else {
      val output = internalRow.get(fieldOrdinal(), dataType)
      output match {
        case array: ArrayData => SingleCartesianData.apply(
          (0 until array.numElements())
            .map(n => array.get(n, dataType.asInstanceOf[ArrayType].elementType)).toList, ordinal())
        case other => SingleCartesianData.apply(List(other), ordinal())
      }
    }
  }
}

case class GetStructFieldsNestNode(field: GetStructField, ordinal: Int) extends GetFieldsNestNode {

  override protected def getParent: Option[Any] = Some(field.child)

  override def selfName(): String = field.name.get

  override def fieldOrdinal(): Int = field.ordinal

  override def fieldDataType(): DataType = {
    field.child.dataType.asInstanceOf[StructType](field.ordinal).dataType
  }
}

case class GetArrayStructFieldsNestNode(field: GetArrayStructFields, ordinal: Int)
  extends GetFieldsNestNode {

  override protected def getParent: Option[Any] = Some(field.child)

  override def selfName(): String = field.field.name

  override def fieldOrdinal(): Int = field.ordinal

  override def fieldDataType(): DataType = field.field.dataType
}

case class BoundReferenceNestNode(boundReference: BoundReference, ordinal: Int)
  extends AbstractNestNode {

  override protected def getParent: Option[Any] = None

  override def selfName(): String = "r"

  override protected def init(): Option[NestNode] = None

  override def isRootNode: Boolean = true

  override def firstNodeName(): String = selfName()

  override def fullFieldName(): String = selfName()

  override def prefixFieldName(): String = ""

  override def childrenNode(): Seq[NestNode] = childNode

  override def addChildNode(getFieldNode: NestNode): Unit = childNode += getFieldNode

  override def canEqual(that: Any): Boolean = {
    that match {
      case b: BoundReferenceNestNode =>
        val otherBoundReference = b.boundReference
        boundReference.ordinal == otherBoundReference.ordinal &&
        boundReference.dataType == otherBoundReference.dataType &&
        boundReference.nullable == boundReference.nullable
      case _ => false
    }
  }


  override def childCartesian(internalRow: InternalRow): CartesianData = {
    val output = boundReference.eval(internalRow)
    boundReference.dataType match {
      case dataType: ArrayType =>
        val arrayData = output.asInstanceOf[ArrayData]
        (0 until arrayData.numElements())
          .map(n => arrayData.get(n, dataType.elementType))
          .map(row => {
            super.childCartesian(row.asInstanceOf[InternalRow])
        }).reduce((c1, c2) => c1.append(c2))
      case _: StructType => super.childCartesian(output.asInstanceOf[InternalRow])
    }
  }

  override def selfCartesian(internalRow: InternalRow): CartesianData = {
    val data = boundReference.eval(internalRow)
    if (data == null) {
      SingleCartesianData.apply(List(null), ordinal)
    }
    else {
      data match {
        case array: ArrayData => SingleCartesianData.apply(array.array.toList, ordinal)
        case other => SingleCartesianData.apply(List(other), ordinal)
      }
    }
  }

  override def fieldDataType(): DataType = boundReference.dataType
}

/**
 * abstraction of the Cartesian product
 */
trait CartesianData {

  /**
   * Repeat n times per row of data
   */
  def repeat(n: Int): CartesianData

  /**
   * Overall data expansion n times
   */
  def expand(n: Int): CartesianData

  /**
   * Append CartesianData, Generate a new CartesianData
   */
  def append(data: CartesianData): CartesianData

  /**
   * Merge CartesianData, Generate a new CartesianData
   */
  def merge(data: CartesianData): CartesianData

  /**
   * Set of serial numbers for each column of data
   */
  def ordinals(): Seq[Int]

  /**
   * Get a single column with the matching serial number
   */
  def getOrdinalCartesianData(ordinal: Int): SingleCartesianData

  /**
   * Data length
   */
  def size(): Int
}

object CartesianData {
  /**
   * Calculate Cartesian product for two data sets
   */
  def reduce(datas: Seq[CartesianData]): CartesianData = {
    datas.reduceLeft((c1, c2) => {
      val size1 = c1.size()
      val size2 = c2.size()
      val expand = c1.expand(size2)
      val repeat = c2.repeat(size1)
      expand.merge(repeat)
    })
  }
}

/**
 * Single column Cartesian product data
 */
case class SingleCartesianData(var data: List[Any], ordinal: Int) extends CartesianData {
  override def repeat(n: Int): SingleCartesianData = {
    SingleCartesianData.apply(data.flatMap(List.fill(n)(_)), ordinal)
  }

  override def expand(n: Int): SingleCartesianData = {
    SingleCartesianData.apply((for (_ <- 1 to n) yield data).flatten.toList, ordinal)
  }

  override def append(singleData: CartesianData): SingleCartesianData = {
    assert(singleData.isInstanceOf[SingleCartesianData] && ordinal == singleData.ordinals().head)
    SingleCartesianData.apply(data ++ singleData.asInstanceOf[SingleCartesianData].data, ordinal)
  }

  override def merge(data: CartesianData): CartesianData = {
    MultiCartesianData.apply(
      data.ordinals().map(ord => data.getOrdinalCartesianData(ord)).toList :+ this
    )
  }

  override def ordinals(): Seq[Int] = {
    Seq(ordinal)
  }

  override def getOrdinalCartesianData(ordinal: Int): SingleCartesianData = {
    if (ordinal == this.ordinal) {
      this
    }
    else {
      throw new IllegalArgumentException(s"$ordinal " +
        s"cant of SingleCartesianData[${this.ordinal}] bounds")
    }
  }

  override def size(): Int = data.size
}

/**
 * Multi-column Cartesian product data
 */
case class MultiCartesianData(multiData: List[SingleCartesianData]) extends CartesianData {
  override def repeat(n: Int): CartesianData = {
    MultiCartesianData.apply(multiData.map(_.repeat(n)))
  }

  override def expand(n: Int): CartesianData = {
    MultiCartesianData.apply(multiData.map(_.expand(n)))
  }

  override def append(data: CartesianData): CartesianData = {
    MultiCartesianData.apply(multiData.map(c => c.append(data.getOrdinalCartesianData(c.ordinal))))
  }

  override def merge(data: CartesianData): CartesianData = {
    MultiCartesianData.apply(
      data.ordinals().map(ord => data.getOrdinalCartesianData(ord)).toList ++ multiData
    )
  }

  override def ordinals(): Seq[Int] = {
    multiData.flatMap(_.ordinals())
  }

  override def getOrdinalCartesianData(ordinal: Int): SingleCartesianData = {
    multiData.find(data => data.ordinal == ordinal)
      .getOrElse(throw new IllegalArgumentException(s"not found $ordinal in ${ordinals()}"))
  }

  override def size(): Int = multiData.head.size()

}