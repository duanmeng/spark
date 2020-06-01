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

package org.apache.spark.network.shuffle;

import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;

public final class DigestShuffleInformation {
  private static final Logger logger = LoggerFactory.getLogger(DigestShuffleInformation.class);
  static final private long DIGEST_LENGTH = 8L;
  static private File getDigestFile(ExecutorShuffleInfo executor, int shuffleId, long mapId) {
    File file = ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
      "shuffle_" + shuffleId + "_" + mapId + "_0.internal.crc");
    return file;
  }

  static long getDigest(
    ExecutorShuffleInfo executor,
    int shuffleId,
    long mapId,
    int reduceId,
    int numBlocks) {
    if (numBlocks != 1) {
      return -1L;
    }
    File digestFile = getDigestFile(executor, shuffleId, mapId);
    if (digestFile.exists()) {
      SeekableByteChannel channel;
      try {
        channel = Files.newByteChannel(digestFile.toPath());
      } catch (IOException e) {
        logger.error("Failed to create a channel on digest file {}", digestFile.toPath());
        return -1L;
      }
      try (DataInputStream in = new DataInputStream(Channels.newInputStream(channel))) {
        channel.position(reduceId * DIGEST_LENGTH);
        return in.readLong();
      } catch (IOException e) {
        logger.error("Fail to read digest from digest file {}", digestFile.toPath());
      }
    } else {
      logger.debug("{} don't exist Digest is not enabled", digestFile.toPath());
    }
    return -1L;
  }
}
