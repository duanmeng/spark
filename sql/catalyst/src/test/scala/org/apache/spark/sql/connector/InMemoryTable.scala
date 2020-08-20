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

package org.apache.spark.sql.connector

import java.time.{Instant, ZoneId}
import java.time.temporal.ChronoUnit
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.scalatest.Assertions._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference, EqualTo => ExpressionEqualTo,
  Expression, IsNotNull => ExpressionIsNotNull, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{BucketTransform, DaysTransform, HoursTransform, IdentityTransform, MonthsTransform, Transform, YearsTransform}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.{And, EqualTo, Filter, IsNotNull}
import org.apache.spark.sql.types.{DataType, DateType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * A simple in-memory table. Rows are stored as a buffered group produced by each output task.
 */
class InMemoryTable(
    val name: String,
    val schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: util.Map[String, String])
  extends Table
    with SupportsRead with SupportsWrite
    with SupportsDelete with SupportsUpdate with SupportsMerge {

  private val allowUnsupportedTransforms =
    properties.getOrDefault("allow-unsupported-transforms", "false").toBoolean

  partitioning.foreach {
    case _: IdentityTransform =>
    case _: YearsTransform =>
    case _: MonthsTransform =>
    case _: DaysTransform =>
    case _: HoursTransform =>
    case _: BucketTransform =>
    case t if !allowUnsupportedTransforms =>
      throw new IllegalArgumentException(s"Transform $t is not a supported transform")
  }

  // The key `Seq[Any]` is the partition values.
  val dataMap: mutable.Map[Seq[Any], BufferedRows] = mutable.Map.empty

  def data: Array[BufferedRows] = dataMap.values.toArray

  def rows: Seq[InternalRow] = dataMap.values.flatMap(_.rows).toSeq

  private val partCols: Array[Array[String]] = partitioning.flatMap(_.references).map { ref =>
    schema.findNestedField(ref.fieldNames(), includeCollections = false) match {
      case Some(_) => ref.fieldNames()
      case None => throw new IllegalArgumentException(s"${ref.describe()} does not exist.")
    }
  }

  private val UTC = ZoneId.of("UTC")
  private val EPOCH_LOCAL_DATE = Instant.EPOCH.atZone(UTC).toLocalDate

  private def getKey(row: InternalRow): Seq[Any] = {
    def extractor(
        fieldNames: Array[String],
        schema: StructType,
        row: InternalRow): (Any, DataType) = {
      val index = schema.fieldIndex(fieldNames(0))
      val value = row.toSeq(schema).apply(index)
      if (fieldNames.length > 1) {
        (value, schema(index).dataType) match {
          case (row: InternalRow, nestedSchema: StructType) =>
            extractor(fieldNames.drop(1), nestedSchema, row)
          case (_, dataType) =>
            throw new IllegalArgumentException(s"Unsupported type, ${dataType.simpleString}")
        }
      } else {
        (value, schema(index).dataType)
      }
    }

    partitioning.map {
      case IdentityTransform(ref) =>
        extractor(ref.fieldNames, schema, row)._1
      case YearsTransform(ref) =>
        extractor(ref.fieldNames, schema, row) match {
          case (days: Int, DateType) =>
            ChronoUnit.YEARS.between(EPOCH_LOCAL_DATE, DateTimeUtils.daysToLocalDate(days))
          case (micros: Long, TimestampType) =>
            val localDate = DateTimeUtils.microsToInstant(micros).atZone(UTC).toLocalDate
            ChronoUnit.YEARS.between(EPOCH_LOCAL_DATE, localDate)
        }
      case MonthsTransform(ref) =>
        extractor(ref.fieldNames, schema, row) match {
          case (days: Int, DateType) =>
            ChronoUnit.MONTHS.between(EPOCH_LOCAL_DATE, DateTimeUtils.daysToLocalDate(days))
          case (micros: Long, TimestampType) =>
            val localDate = DateTimeUtils.microsToInstant(micros).atZone(UTC).toLocalDate
            ChronoUnit.MONTHS.between(EPOCH_LOCAL_DATE, localDate)
        }
      case DaysTransform(ref) =>
        extractor(ref.fieldNames, schema, row) match {
          case (days, DateType) =>
            days
          case (micros: Long, TimestampType) =>
            ChronoUnit.DAYS.between(Instant.EPOCH, DateTimeUtils.microsToInstant(micros))
        }
      case HoursTransform(ref) =>
        extractor(ref.fieldNames, schema, row) match {
          case (micros: Long, TimestampType) =>
            ChronoUnit.HOURS.between(Instant.EPOCH, DateTimeUtils.microsToInstant(micros))
        }
      case BucketTransform(numBuckets, ref) =>
        (extractor(ref.fieldNames, schema, row).hashCode() & Integer.MAX_VALUE) % numBuckets
    }
  }

  private def getFieldIndex(fieldName: String): Int = schema.names.indexOf(fieldName)

  def withData(data: Array[BufferedRows]): InMemoryTable = dataMap.synchronized {
    data.foreach(_.rows.foreach { row =>
      val key = getKey(row)
      dataMap += dataMap.get(key)
        .map(key -> _.withRow(row))
        .getOrElse(key -> new BufferedRows().withRow(row))
    })
    this
  }

  override def capabilities: util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC,
    TableCapability.TRUNCATE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    () => new InMemoryBatchScan(data.map(_.asInstanceOf[InputPartition]))
  }

  class InMemoryBatchScan(data: Array[InputPartition]) extends Scan with Batch {
    override def readSchema(): StructType = schema

    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = data

    override def createReaderFactory(): PartitionReaderFactory = BufferedRowsReaderFactory
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryTable.maybeSimulateFailedTableWrite(info.options)

    new WriteBuilder with SupportsTruncate with SupportsOverwrite with SupportsDynamicOverwrite {
      private var writer: BatchWrite = Append

      override def truncate(): WriteBuilder = {
        assert(writer == Append)
        writer = TruncateAndAppend
        this
      }

      override def overwrite(filters: Array[Filter]): WriteBuilder = {
        assert(writer == Append)
        writer = new Overwrite(filters)
        this
      }

      override def overwriteDynamicPartitions(): WriteBuilder = {
        assert(writer == Append)
        writer = DynamicOverwrite
        this
      }

      override def buildForBatch(): BatchWrite = writer
    }
  }

  private abstract class TestBatchWrite extends BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      BufferedRowsWriterFactory
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  }

  private object Append extends TestBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }
  }

  private object DynamicOverwrite extends TestBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      dataMap --= newData.flatMap(_.rows.map(getKey))
      withData(newData)
    }
  }

  private class Overwrite(filters: Array[Filter]) extends TestBatchWrite {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val deleteKeys = InMemoryTable.filtersToKeys(
        dataMap.keys, partCols.map(_.toSeq.quoted), filters)
      dataMap --= deleteKeys
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }
  }

  private object TruncateAndAppend extends TestBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      dataMap.clear
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }
  }

  private def partitionNames(): Seq[String] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    partCols.map(_.toSeq.quoted)
  }

  override def deleteWhere(deleteExpression: Expression): Unit = {

    deleteWhere(this.dataMap, deleteExpression)
  }


  private def deleteWhere(
    dataMap: mutable.Map[Seq[Any], BufferedRows], deleteExpression: Expression):
    Unit = dataMap.synchronized {
    val deleteKeys = InMemoryTable.filtersToKeys(dataMap.keys, partitionNames(), deleteExpression)
    dataMap --= deleteKeys
  }

  override def update(
    assignments: util.Map[String, Expression],
    updateExpression: Expression): Unit = dataMap.synchronized {
    update(this.dataMap, assignments, updateExpression)
  }

  private def update(
    dataMap: mutable.Map[Seq[Any], BufferedRows],
    assignments: util.Map[String, Expression],
    updateExpressions: Expression): Unit = dataMap.synchronized {
    // The implementation is only target on if update process can work
    val matchedKeys = InMemoryTable.filtersToKeys(
      dataMap.keys, partitionNames(), updateExpressions).toSet
    dataMap.filter { case (key, _) => matchedKeys.contains(key) }
      .values.foreach {
        _.rows.foreach { row =>
          assignments.asScala.foreach { assignment =>
            row.setInt(
              getFieldIndex(assignment._1),
              assignment._2.asInstanceOf[Literal].value.asInstanceOf[Int])
          }
      }
    }
  }

  def insert(
    sourceTable: InMemoryTable,
    matchedKeys: Set[Seq[Any]],
    assignments: util.Map[String, Expression],
    insertExpression: Expression): Unit = dataMap.synchronized {
    // The implementation is only target on if insert process can work
    val filteredKeys = InMemoryTable
      .filtersToKeys(sourceTable.dataMap.keys, sourceTable.partitionNames(), insertExpression).toSet
    sourceTable.dataMap.keys.foreach { key =>
      if (!matchedKeys.contains(key) && filteredKeys.contains(key)) {
        val data = sourceTable.dataMap.getOrElse(
          key, throw new IllegalArgumentException(s"${key.toString} should exist"))
        dataMap(key) = data
      }
    }
  }

  def getAttributeValues(attr: String): Set[Any] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    val partitionNames = partCols.map(_.toSeq.quoted)
    dataMap.keys.map { partValues =>
      InMemoryTable.extractValue(attr, partitionNames, partValues)
    }.toSet
  }

  def splitMatch(filters: Array[Filter]):
    (mutable.Map[Seq[Any], BufferedRows],
    mutable.Map[Seq[Any], BufferedRows]) = dataMap.synchronized {
    val matchedKeys =
      InMemoryTable.filtersToMergeKeys(dataMap.keys, partitionNames(), filters)
    val unmatchedDataMap = dataMap -- matchedKeys
    val matchedDataMap = dataMap -- unmatchedDataMap.keys
    (matchedDataMap, unmatchedDataMap)
  }

  def splitMatch(expressions: Seq[Expression]):
  (mutable.Map[Seq[Any], BufferedRows],
    mutable.Map[Seq[Any], BufferedRows]) = dataMap.synchronized {
    val matchedKeys =
      InMemoryTable.filtersToMergeKeys(dataMap.keys, partitionNames(), expressions)
    val unmatchedDataMap = dataMap -- matchedKeys
    val matchedDataMap = dataMap -- unmatchedDataMap.keys
    (matchedDataMap, unmatchedDataMap)
  }

  def mergeIntoWithTable(
    sourceTable: Table,
    targetAlias: String,
    sourceTableName: String,
    sourceAlias: String,
    mergeCondition: Expression,
    updateAssignments: util.Map[String, Expression],
    insertAssignments: util.Map[String, Expression],
    deleteExpression: Expression,
    updateExpression: Expression,
    insertExpression: Expression): Unit = {
    val (matchedDataMap, unmatchedDataMap) = mergeCondition match {
      case ExpressionEqualTo(
      AttributeReference(attr, dataType, nullable, metadata), AttributeReference(_, _, _, _)) =>
        val expressions = sourceTable match {
          case st: InMemoryTable =>
            st.getAttributeValues(attr)
              .map { value =>
                  ExpressionEqualTo(
                    AttributeReference(
                      attr, dataType, nullable, metadata)().asInstanceOf[Expression],
                    Literal(value, dataType).asInstanceOf[Expression])
              }
              .toArray
          case _ => throw new IllegalArgumentException(
            s"Unsupported source table must be InMemoryTable when using InMemoryTable")
        }
        splitMatch(expressions)
      case e: ExpressionIsNotNull =>
        splitMatch(Seq(e))
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported merge expression in InMemoryTable: $mergeCondition")
    }
    dataMap.clear()
    val matchedKeys = matchedDataMap.keys.toSet
    deleteWhere(matchedDataMap, deleteExpression)
    update(matchedDataMap, updateAssignments, updateExpression)
    insert(
      sourceTable.asInstanceOf[InMemoryTable],
      matchedKeys,
      insertAssignments,
      insertExpression)
    dataMap.synchronized {
      matchedDataMap.foreach { case (key, value) =>
        dataMap(key) = value
      }
      unmatchedDataMap.foreach { case (key, value) =>
        dataMap(key) = value
      }
    }
  }

  def mergeIntoWithQuery(
    targetAlias: String,
    sourceQuery: String,
    sourceAlias: String,
    mergeCondition: Expression,
    updateAssignments: util.Map[String, Expression],
    insertAssignments: util.Map[String, Expression],
    deleteExpression: Expression,
    updateExpression: Expression,
    insertExpression: Expression): Unit = {
    throw new UnsupportedOperationException(s"Subquery is unsupported in InMemoryTable")
  }
}

object InMemoryTable {
  val SIMULATE_FAILED_WRITE_OPTION = "spark.sql.test.simulateFailedWrite"

  def filtersToKeys(
    keys: Iterable[Seq[Any]],
    partitionNames: Seq[String],
    expression: Expression): Iterable[Seq[Any]] = {
    keys.filter { partValues =>
      expression match {
        case ExpressionEqualTo(AttributeReference(attr, _, _, _), Literal(value, _)) =>
          val actualValue = extractValue(attr, partitionNames, partValues)
          if (actualValue.isInstanceOf[UTF8String]) {
            value == actualValue.toString
          } else {
            value == actualValue
          }
        case ExpressionIsNotNull(AttributeReference(attr, _, _, _)) =>
          null != extractValue(attr, partitionNames, partValues)
        case null => true
        case f =>
          throw new IllegalArgumentException(s"Unsupported expression type: $f")
      }
    }
  }

  def filtersToKeys(
      keys: Iterable[Seq[Any]],
      partitionNames: Seq[String],
      filters: Array[Filter]): Iterable[Seq[Any]] = {
    keys.filter { partValues =>
      filters.flatMap(splitAnd).forall {
        case EqualTo(attr, value) =>
          val actualValue = extractValue(attr, partitionNames, partValues)
          if (actualValue.isInstanceOf[UTF8String]) {
            value == actualValue.toString
          } else {
            value == actualValue
          }
        case IsNotNull(attr) =>
          null != extractValue(attr, partitionNames, partValues)
        case f =>
          throw new IllegalArgumentException(s"Unsupported filter type: $f")
      }
    }
  }

  def filtersToMergeKeys(
    keys: Iterable[Seq[Any]],
    partitionNames: Seq[String],
    filters: Array[Filter]): Iterable[Seq[Any]] = {
    keys.filter { partValues =>
      filters.flatMap(splitAnd).exists {
        case EqualTo(attr, value) =>
          value == extractValue(attr, partitionNames, partValues)
        case IsNotNull(attr) =>
          null != extractValue(attr, partitionNames, partValues)
        case f =>
          throw new IllegalArgumentException(s"Unsupported filter type: $f")
      }
    }
  }

  def filtersToMergeKeys(
    keys: Iterable[Seq[Any]],
    partitionNames: Seq[String],
    expressions: Seq[Expression]): Iterable[Seq[Any]] = {
    keys.filter { partValues =>
      expressions.exists {
        case ExpressionEqualTo(AttributeReference(attr, _, _, _), Literal(value, _)) =>
          val actualValue = extractValue(attr, partitionNames, partValues)
          if (actualValue.isInstanceOf[UTF8String]) {
            value == actualValue.toString
          } else {
            value == actualValue
          }
        case ExpressionIsNotNull(AttributeReference(attr, _, _, _)) =>
          null != extractValue(attr, partitionNames, partValues)
        case null => true
        case f =>
          throw new IllegalArgumentException(s"Unsupported expression type: $f")
      }
    }
  }

  def extractValue(
      attr: String,
      partFieldNames: Seq[String],
      partValues: Seq[Any]): Any = {
    partFieldNames.zipWithIndex.find(_._1 == attr) match {
      case Some((_, partIndex)) =>
        partValues(partIndex)
      case _ =>
        throw new IllegalArgumentException(s"Unknown filter attribute: $attr")
    }
  }

  def updateValue(
    attr: String,
    partFieldNames: Seq[String],
    partValues: Seq[Any]): Any = {
    partFieldNames.zipWithIndex.find(_._1 == attr) match {
      case Some((_, partIndex)) =>
        partValues(partIndex)
      case _ =>
        throw new IllegalArgumentException(s"Unknown filter attribute: $attr")
    }
  }
  private def splitAnd(filter: Filter): Seq[Filter] = {
    filter match {
      case And(left, right) => splitAnd(left) ++ splitAnd(right)
      case _ => filter :: Nil
    }
  }

  def maybeSimulateFailedTableWrite(tableOptions: CaseInsensitiveStringMap): Unit = {
    if (tableOptions.getBoolean(SIMULATE_FAILED_WRITE_OPTION, false)) {
      throw new IllegalStateException("Manual write to table failure.")
    }
  }
}

class BufferedRows extends WriterCommitMessage with InputPartition with Serializable {
  val rows = new mutable.ArrayBuffer[InternalRow]()

  def withRow(row: InternalRow): BufferedRows = {
    rows.append(row)
    this
  }
}

private object BufferedRowsReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new BufferedRowsReader(partition.asInstanceOf[BufferedRows])
  }
}

private class BufferedRowsReader(partition: BufferedRows) extends PartitionReader[InternalRow] {
  private var index: Int = -1

  override def next(): Boolean = {
    index += 1
    index < partition.rows.length
  }

  override def get(): InternalRow = partition.rows(index)

  override def close(): Unit = {}
}

private object BufferedRowsWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new BufferWriter
  }
}

private class BufferWriter extends DataWriter[InternalRow] {
  private val buffer = new BufferedRows

  override def write(row: InternalRow): Unit = buffer.rows.append(row.copy())

  override def commit(): WriterCommitMessage = buffer

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
