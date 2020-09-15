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