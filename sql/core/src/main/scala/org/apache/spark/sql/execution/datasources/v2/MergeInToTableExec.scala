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

import java.util

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.catalog.SupportsMerge
import org.apache.spark.sql.execution.LeafExecNode

case class MergeInToTableExec(
  table: SupportsMerge,
  sourceTable: SupportsMerge,
  targetAlias: String,
  sourceTableName: Option[String],
  sourceQuery: Option[String],
  sourceAlias: String,
  mergeCondition: Expression,
  updateAssignments: util.Map[String, Expression],
  insertAssignments: util.Map[String, Expression],
  deleteExpression: Expression,
  updateExpression: Expression,
  insertExpression: Expression)
  extends V2CommandExec with LeafExecNode {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {

    if (sourceTableName.isDefined) {
      table.mergeIntoWithTable(
        sourceTable,
        targetAlias,
        sourceTableName.get,
        sourceAlias,
        mergeCondition,
        updateAssignments,
        insertAssignments,
        deleteExpression,
        updateExpression,
        insertExpression)
    } else if (sourceQuery.isDefined) {
      table.mergeIntoWithQuery(
        targetAlias,
        sourceQuery.get,
        sourceAlias,
        mergeCondition,
        updateAssignments,
        insertAssignments,
        deleteExpression,
        updateExpression,
        insertExpression)
    } else {
      throw new SparkException(s"sourceTableName or SourceQuery should be set with Merge action")
    }

    Seq.empty
  }

}
