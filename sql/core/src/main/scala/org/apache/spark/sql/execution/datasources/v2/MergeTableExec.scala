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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.catalog.SupportsMerge
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.sources.Filter

case class MergeTableExec(
  table: SupportsMerge,
  targetAlias: String,
  sourceTable: Option[String],
  sourceQuery: Option[String],
  sourceAlias: String,
  mergeFilters: Array[Filter],
  deleteFilters: Array[Filter],
  updateFilters: Array[Filter],
  updateAssignments: Map[String, Expression],
  insertFilters: Array[Filter],
  insertAssignments: Map[String, Expression],
  sTable: SupportsMerge) extends LeafExecNode {

  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {

    if (sourceTable.isDefined) {
      table.mergeIntoWithTable(
        targetAlias,
        sourceTable.get,
        sourceAlias,
        mergeFilters,
        deleteFilters,
        updateFilters,
        updateAssignments.asJava,
        insertFilters,
        insertAssignments.asJava,
        sTable)
    } else if (sourceQuery.isDefined) {
      table.mergeIntoWithQuery(
        targetAlias,
        sourceQuery.get,
        sourceAlias,
        mergeFilters,
        deleteFilters,
        updateFilters,
        updateAssignments.asJava,
        insertFilters,
        insertAssignments.asJava)
    } else {
      throw new SparkException(s"SourceTable or SourceQuery should be set with Merge action")
    }

    sparkContext.emptyRDD
  }

}
