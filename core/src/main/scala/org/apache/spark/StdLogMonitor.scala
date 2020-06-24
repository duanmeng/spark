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

package org.apache.spark

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.channels.FileChannel
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Monitor the size of stdout and stderr log
 */
private[spark] class StdLogMonitor(sparkConf: SparkConf) extends Logging {

  private val monitorThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("std-log-monitor-thread")

  private var timeoutCheckingTask: ScheduledFuture[_] = _

  private val checkTimeoutIntervalMs =
    sparkConf.getTimeAsMs("spark.log.check.intervalMs", "3000ms")

  private val stdoutLogThreshold =
    sparkConf.getLong("spark.stdout.log.threshold", 10 * 1024 * 1024L) // 10MB

  private val stderrLogThreshold =
    sparkConf.getLong("spark.stderr.log.threshold", 10 * 1024 * 1024L) // 10MB

  private val stdoutFilePath =
    sparkConf.get("spark.stdout.log.path", "/tmp")
  logInfo(s"stdout: $stdoutFilePath")

  private val stderrFilePath =
    sparkConf.get("spark.stderr.log.path", "/tmp")
  logInfo(s"stderr: $stderrFilePath")

  /**
   * Monitor process
   */
  private def process(): Unit = {
    def processImpl(file: File, threshold: Long): Unit = {
      if (file.exists() && file.isFile) {
        if (file.length() > threshold) {
          logWarning(s"Log size exceeded the maximum size(${threshold/1024}KB).")
          val solidFile = new File(file.getAbsolutePath + ".solid")
          var inputChannel: FileChannel = null
          var outputChannel: FileChannel = null
          try {
            inputChannel = new FileInputStream(file).getChannel
            outputChannel = new FileOutputStream(solidFile).getChannel
            outputChannel.transferFrom(inputChannel, 0, threshold)
          } finally {
            inputChannel.close()
            outputChannel.close()
          }
          file.delete()
        }
      }
    }

    processImpl(new File(stdoutFilePath), stdoutLogThreshold)
    processImpl(new File(stderrFilePath), stderrLogThreshold)
  }

  def start(): Unit = {
    timeoutCheckingTask = monitorThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        Utils.tryLogNonFatalError {
          process()
        }
      }
    }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    monitorThread.shutdownNow()
    process() // final check hook
  }

}
