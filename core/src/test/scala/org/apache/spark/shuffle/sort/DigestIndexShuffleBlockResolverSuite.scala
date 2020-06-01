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

package org.apache.spark.shuffle.sort

import java.io.{ByteArrayInputStream, DataInputStream, File, FileInputStream, FileOutputStream}

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.buffer.{DigestFileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.util.DigestUtils
import org.apache.spark.shuffle.DigestIndexShuffleBlockResolver
import org.apache.spark.storage._
import org.apache.spark.util.Utils

class DigestIndexShuffleBlockResolverSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _

  private var tempDir: File = _
  private val conf: SparkConf = new SparkConf(loadDefaults = false)

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    MockitoAnnotations.initMocks(this)

    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(diskBlockManager.getFile(any[BlockId])).thenAnswer(
      (invocation: InvocationOnMock) => new File(tempDir, invocation.getArguments.head.toString))
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  test("check index file, data file and digest file existence and length") {
    val shuffleId = 1
    val mapId = 2
    val idxName = s"shuffle_${shuffleId}_${mapId}_0.index"
    val digestName = s"shuffle_${shuffleId}_${mapId}_0.internal.crc"
    val resolver = new DigestIndexShuffleBlockResolver(conf, blockManager)
    val lengths = Array[Long](10, 0, 20)
    val dataTmp = File.createTempFile("shuffle", null, tempDir)
    val out = new FileOutputStream(dataTmp)
    Utils.tryWithSafeFinally {
      out.write(new Array[Byte](30))
    } {
      out.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    val indexFile = new File(tempDir.getAbsolutePath, idxName)
    val digestFile = new File(tempDir.getAbsolutePath, digestName)
    val dataFile = resolver.getDataFile(shuffleId, mapId)

    assert(indexFile.exists())
    assert(indexFile.length() === (lengths.length + 1) * 8)
    assert(dataFile.exists())
    assert(dataFile.length() === 30)
    assert(!dataTmp.exists())
    assert(digestFile.exists())
    assert(digestFile.length() == lengths.length * 8L)

    val lengths2 = new Array[Long](3)
    val dataTmp2 = File.createTempFile("shuffle", null, tempDir)
    val out2 = new FileOutputStream(dataTmp2)
    Utils.tryWithSafeFinally {
      out2.write(Array[Byte](1))
      out2.write(new Array[Byte](29))
    } {
      out2.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths2, dataTmp2)

    assert(indexFile.length() === (lengths.length + 1) * 8)
    assert(lengths2.toSeq === lengths.toSeq)
    assert(dataFile.exists())
    assert(dataFile.length() === 30)
    assert(!dataTmp2.exists())

    // The dataFile should be the previous one
    val firstByte = new Array[Byte](1)
    val dataIn = new FileInputStream(dataFile)
    Utils.tryWithSafeFinally {
      dataIn.read(firstByte)
    } {
      dataIn.close()
    }
    assert(firstByte(0) === 0)

    // The index file should not change
    val indexIn = new DataInputStream(new FileInputStream(indexFile))
    Utils.tryWithSafeFinally {
      indexIn.readLong() // the first offset is always 0
      assert(indexIn.readLong() === 10, "The index file should not change")
    } {
      indexIn.close()
    }

    // remove data file
    dataFile.delete()
  }

  test("check digest file and digest value for each segment") {
    val confClone = conf.clone
    // confClone.set(config.SHUFFLE_INTERNAL_DIGEST_ENABLED, true)
    val resolver = new DigestIndexShuffleBlockResolver(confClone, blockManager)
    val lengths = Array[Long](10, 0, 20)
    val dataTmp = File.createTempFile("shuffle", null, tempDir)
    val out = new FileOutputStream(dataTmp)
    Utils.tryWithSafeFinally {
      out.write(new Array[Byte](30))
    } {
      out.close()
    }
    def getDigest(buf: ManagedBuffer): Long = {
      assert(buf.isInstanceOf[DigestFileSegmentManagedBuffer]);
      buf.asInstanceOf[DigestFileSegmentManagedBuffer].getDigest
    }
    val digest0 = DigestUtils.getDigest(new ByteArrayInputStream(new Array[Byte](10)))
    resolver.writeIndexFileAndCommit(1, 2, lengths, dataTmp)
    val buf0 = resolver.getBlockData(ShuffleBlockId(1, 2, 0))
    assert(getDigest(buf0) == digest0)
    assert(getDigest(buf0) != -1)
    val digest1 = -1L
    val buf1 = resolver.getBlockData(ShuffleBlockId(1, 2, 1))
    assert(getDigest(buf1) == digest1)
    val digest2 = DigestUtils.getDigest(new ByteArrayInputStream(new Array[Byte](20)))
    val buf2 = resolver.getBlockData(ShuffleBlockId(1, 2, 2))
    assert(getDigest(buf2) == digest2)
  }
}
