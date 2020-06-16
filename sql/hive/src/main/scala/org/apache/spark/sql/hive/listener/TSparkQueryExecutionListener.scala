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

package org.apache.spark.sql.hive.listener

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{And, Expression, InSubquery, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.{EventLoop, Utils}

class TSparkQueryExecutionListener(conf: SparkConf) extends QueryExecutionListener with Logging {

  private val sdfTime = new SimpleDateFormat("yyyyMMddHHmmss")
  private val sdfDate = new SimpleDateFormat("yyyyMMdd")
  private val DEFAULT_AUDIT_PLATFORM = "SparkSQL"
  private val ACTION_READ = "read"
  private val ACTION_WRITE = "write"
  private val STATUS_SUCCESS = "success"
  private val STATUS_FAIL = "fail"
  private var ts: TubeSender = _

  private val eventLoop = new EventLoop[Set[SQLAuditInfo]]("TDBankEventLoop") {

    override def onReceive(event: Set[SQLAuditInfo]): Unit = {
      // send audit log to TDBank, retry 3 times if necessary, won't block the driver
      writeSQLAuditInfos(event)
    }

    override def onError(e: Throwable): Unit = {
      // ignore the error here, actually, onReceive will handle all the exception
    }

    override def onStart(): Unit = {
      ts = new TubeSender(conf)
      ts.start
    }
  }

  if (!Utils.isTesting) {
    // start a thread for sending audit log to TDBank asynchronous
    eventLoop.start
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long,
      sqlText: String): Unit = {
    try {
      eventLoop.post(createSQLAuditInfos(qe.analyzed, STATUS_SUCCESS, durationNs, sqlText))
    } catch {
      case e: Throwable => logWarning(s"Fail to send audit log to TDBank.", e)
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception,
      sqlText: String): Unit = {
    try {
      eventLoop.post(createSQLAuditInfos(qe.analyzed, STATUS_FAIL, 0, sqlText))
    } catch {
      case e: Throwable => logWarning("Fail to send audit log to TDBank.", e)
    }
  }

  private[listener] def getQueryInfos(plan: LogicalPlan): (String, Set[String], Set[String]) = {
    plan match {
      // currently, create/drop/insert is not supported for SparkSQL + TDW
      case CreateHiveTableAsSelectCommand(tableDesc, query, _, _) =>
        ("CreateHiveTableAsSelect", getTables(query), Set(tableDesc.identifier.unquotedString))
      case InsertIntoHiveTable(table, _, query, _, _, _) =>
        ("InsertIntoHiveTable", getTables(query), Set(table.identifier.unquotedString))
      case CreateDataSourceTableAsSelectCommand(table, _, query, _) =>
        ("CreateDataSourceTableAsSelect", getTables(query), Set(table.identifier.unquotedString))
      case c: InsertIntoHadoopFsRelationCommand =>
        ("InsertIntoHadoopFsRelation", getTables(c.query), Set(c.outputPath.toString))
      case DropDatabaseCommand(databaseName, _, _) =>
        ("DropDatabase", Set.empty[String], Set(databaseName))
      case d: DropTableCommand =>
        ("DropTable", Set.empty[String], Set(d.tableName.unquotedString))
      case CreateTableCommand(table, _) =>
        ("CreateTable", Set.empty[String], Set(table.identifier.unquotedString))
      case CreateTableLikeCommand(targetTable, sourceTable, _, _) =>
        ("CreateTableLike", Set(sourceTable.unquotedString), Set(targetTable.unquotedString))
      case _ =>
        ("Query", getTables(plan), Set.empty[String])
    }
  }

  private def writeSQLAuditInfos(sqlAuditInfos: Set[SQLAuditInfo]): Unit = {
    if (sqlAuditInfos.isEmpty) {
      // there is no resource for audit log
      return
    }

    val messages = sqlAuditInfos.map(auditoInfo => auditoInfo.toTDBankMessage).toList
    ts.sendMessageSync(messages)
  }

  private def getTables(plan: LogicalPlan): Set[String] = {
    var tableSet = Set.empty[String]
    plan.foreach {
      case Filter(condition, _) => tableSet ++= getTablesFromExpression(condition)
      case SubqueryAlias(name, HiveTableRelation(_, _, _) | LogicalRelation(_, _, _, _)) =>
        tableSet += name.unquotedString
      case _ =>
    }
    tableSet
  }

  private def getTablesFromExpression(expr: Expression): Set[String] = {
    expr match {
      case And(l, r) => getTablesFromExpression(l) ++ getTablesFromExpression(r)
      case Or(l, r) => getTablesFromExpression(l) ++ getTablesFromExpression(r)
      case Not(e) => getTablesFromExpression(e)
      case InSubquery(_, p) => getTables(p.plan)
      case _ => Set.empty[String]
    }
  }

  // visible for test
  private[listener] def createSQLAuditInfos(
      plan: LogicalPlan,
      status: String,
      duration: Long,
      comment: String): Set[SQLAuditInfo] = {
    val auditBatch = UUID.randomUUID.toString
    val user = UserGroupInformation.getCurrentUser.getShortUserName
    val now = Calendar.getInstance.getTime
    val auditTime = sdfTime.format(now)
    val auditDate = sdfDate.format(now)
    // convert to second
    val durationSec = TimeUnit.NANOSECONDS.toSeconds(duration)
    val (command, readResources, writeResources) = getQueryInfos(plan)
    // replace tab with space because tab is the split for message
    val c = if (comment == null) {
      command
    } else {
      comment.replace("\t", " ")
    }
    createSQLAuditInfosByType(auditBatch, auditTime, auditDate, user, readResources,
      ACTION_READ, status, durationSec, c) ++ createSQLAuditInfosByType(auditBatch,
      auditTime, auditDate, user, writeResources, ACTION_WRITE, status, durationSec, c)
  }

  private def createSQLAuditInfosByType(
      auditBatch: String,
      auditTime: String,
      auditDate: String,
      user: String,
      resources: Set[String],
      action: String,
      status: String,
      duration: Long,
      comment: String): Set[SQLAuditInfo] = {
    val platform = conf.get("spark.sql.audit.platform", DEFAULT_AUDIT_PLATFORM)
    resources.map { resource =>
      new SQLAuditInfo(UUID.randomUUID.toString, auditDate, auditTime, platform,
        auditBatch, user, resource, action, status, duration, comment)
    }
  }
}

class SQLAuditInfo(
    val auditId: String,
    val auditDate: String,
    val auditTime: String,
    val auditPlatform: String,
    val auditBatch: String,
    val user: String,
    val resource: String,
    val action: String,
    val status: String,
    val duration: Long,
    val comment: String) {

  def toTDBankMessage: Array[Byte] = {
    List(auditId, auditDate, auditTime, auditPlatform, auditBatch, user,
      resource, action, status, duration.toString, comment).mkString("\t").getBytes("utf8")
  }

  // equals and hashCode are only for test case
  override def equals(other: Any): Boolean = {
    other match {
      case sai: SQLAuditInfo =>
        action == sai.action && resource == sai.resource && auditDate == sai.auditDate &&
          duration == sai.duration && comment == sai.comment && status == sai.status &&
          auditPlatform == sai.auditPlatform
      case _ => false
    }
  }

  override def hashCode(): Int = {
    comment.hashCode
  }
}
