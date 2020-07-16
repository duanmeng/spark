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

package org.apache.spark.shuffle

import java.io.{BufferedOutputStream, DataInputStream, DataOutputStream, File, FileInputStream, FileOutputStream, IOException}
import java.nio.channels.Channels
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{DigestFileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.ExecutorDiskUtils
import org.apache.spark.network.util.{DigestUtils, LimitedInputStream}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockBatchId, ShuffleBlockId, ShuffleDigestBlockId, ShuffleIndexBlockId}
import org.apache.spark.util.Utils

private[spark] class DigestIndexShuffleBlockResolver(
  conf: SparkConf,
  _blockManager: BlockManager = null)
  extends IndexShuffleBlockResolver(conf, _blockManager)
  with Logging {

  /**
   * Get the shuffle digest file.
   *
   * When the dirs parameter is None then use the disk manager's local directories. Otherwise,
   * read from the specified directories.
   */
  def getDigestFile(
      shuffleId: Int,
      mapId: Long,
      dirs: Option[Array[String]] = None): File = {
    val blockId = ShuffleDigestBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    dirs
      .map(ExecutorDiskUtils.getFile(_, blockManager.subDirsPerLocalDir, blockId.name))
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }

  private final val digestLength = DigestUtils.getDigestLength

  /**
   * Remove data file and index file that contain the output data from one map.
   */
  override def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    super.removeDataByMap(shuffleId, mapId)
    val file = getDigestFile(shuffleId, mapId);
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath}")
      }
    }
  }

  /**
   * Generate digest for each partition in data file.
   */
  private def generateDigests(dataTmp: File, lengths: Array[Long]): Array[Long] = {
    val digests = new Array[Long](lengths.length)
    val dateIn = if (dataTmp != null && dataTmp.exists()) {
      new FileInputStream(dataTmp)
    } else {
      null
    }
    Utils.tryWithSafeFinally {
      if (dateIn != null) {
        lengths.indices.foreach { i =>
          val length = lengths(i)
          if (length == 0) {
            digests(i) = -1L
          } else {
            digests(i) = DigestUtils.getDigest(new LimitedInputStream(dateIn, length))
          }
        }
      }
    } {
      if (dateIn != null) {
        dateIn.close()
      }
    }
    digests
  }

  protected def writeDigestFile(
    digestFile: File,
    digests: Array[Long]): Unit = {
    val digestTmp = Utils.tempFileWith(digestFile)
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(digestTmp)))
    Utils.tryWithSafeFinally {
      digests.foreach { digest =>
        out.writeLong(digest)
      }
    } {
      out.close()
    }
    Utils.tryWithSafeFinally {
      if (digestFile.exists()) {
        digestFile.delete()
      }
      if (!digestTmp.renameTo(digestFile)) {
        throw new IOException(s"Fail to rename file ${digestTmp.toPath} to ${digestFile.toPath}")
      }
    } {
      if (digestTmp.exists() && !digestTmp.delete()) {
        logError(s"Failed to delete temporary digest file at ${digestTmp.getAbsolutePath}")
      }
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   */
  override def writeIndexFileAndCommit(
         shuffleId: Int,
         mapId: Long,
         lengths: Array[Long],
         dataTmp: File): Unit = {
    if (dataTmp == null) {
      super.writeIndexFileAndCommit(shuffleId, mapId, lengths, null)
      return
    }
    val indexFile = getIndexFile(shuffleId, mapId)
    val digestFile = getDigestFile(shuffleId, mapId)
    val dataFile = getDataFile(shuffleId, mapId)
    // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
    // the following check and rename are atomic.
    synchronized {
      val digests = generateDigests(dataTmp, lengths)
      logDebug(s"Digest ${digests.map{_.toString}.mkString(",")} " +
        s"from ${dataTmp.toPath} mapId: $mapId " +
        s"lengths: ${lengths.map{_.toString}.mkString(",")}")
      val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
      if (existingLengths != null) {
        // Another attempt for the same task has already written our map outputs successfully,
        // so just use the existing partition lengths and delete our temporary map outputs.
        System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
        if (dataTmp != null && dataTmp.exists()) {
          dataTmp.delete()
        }
      } else {
        writeIndexFile(indexFile, lengths)
        if (dataFile.exists()) {
          dataFile.delete()
        }
        if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
          throw new IOException(s"Fail to rename file ${dataTmp.toPath} to ${dataFile.toPath}")
        }
      }
      writeDigestFile(digestFile, digests)
    }
  }

  private def getDigestForBlockData(
      startReduceId: Int,
      endReduceId: Int,
      file: File): Long = {
    if (endReduceId > startReduceId + 1) {
      logWarning("In adaptive mode multiple partitions may be insert into one segment buf " +
        s"but the digest is generated and checked per partition so ignore digest " +
        s"for ${file.toPath} from partition($startReduceId) to partition($endReduceId).")
      -1L
    } else {
      val digestChannel = Files.newByteChannel(file.toPath)
      val dg = new DataInputStream(Channels.newInputStream(digestChannel))
      try {
        digestChannel.position(startReduceId * 8L)
        val digest = dg.readLong
        val actualPosition = digestChannel.position
        val expectedPosition = (startReduceId + 1L) * digestLength
        if (actualPosition != expectedPosition) {
          throw new IllegalStateException(
            s"SPARK-22982: Incorrect channel position after $startReduceId " +
              s"multiply $digestLength digest file $file reads: " +
              s"expected $expectedPosition but actual position was $actualPosition.")
        }
        digest
      } finally {
        dg.close()
      }
    }
  }

  private def getIndexLengthsAndOffset(
      startReduceId: Int,
      endReduceId: Int,
      file: File): (Long, Long) = {
    val indexChannel = Files.newByteChannel(file.toPath)
    val in = new DataInputStream(Channels.newInputStream(indexChannel))
    try {
      // calculate offset using offsets in index file
      indexChannel.position(startReduceId * 8L)
      val startOffset = in.readLong
      indexChannel.position(endReduceId * 8L)
      val nextOffset = in.readLong
      val actualPosition = indexChannel.position
      val expectedPosition = endReduceId * 8L + 8
      if (actualPosition != expectedPosition) {
        throw new IllegalStateException(
          s"SPARK-22982: Incorrect channel position after index file reads: " +
            s"expected $expectedPosition but actual position was $actualPosition.")
      }
      (startOffset, nextOffset)
    } finally {
      in.close()
    }
  }

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
    }
    val digestFile = getDigestFile(shuffleId, mapId)
    val indexFile = getIndexFile(shuffleId, mapId)
    val digest = getDigestForBlockData(startReduceId, endReduceId, digestFile)
    val (startOffset, nextOffset) =
      getIndexLengthsAndOffset(startReduceId, endReduceId, indexFile)

    new DigestFileSegmentManagedBuffer(
      transportConf,
      getDataFile(shuffleId, mapId),
      startOffset,
      nextOffset - startOffset,
      digest)
  }

  override def stop(): Unit = {}

}
