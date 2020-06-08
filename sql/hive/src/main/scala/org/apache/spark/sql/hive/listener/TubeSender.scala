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

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.tencent.tdbank.busapi.{BusClientConfig, DefaultMessageSender, SendResult}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

class TubeSender(conf: SparkConf) extends Logging {

  private var sender: DefaultMessageSender = _
  private val DEFAULT_TUBE_SENDER_TIMEOUT_MS = 5000
  private val DEFAULT_TUBE_SENDER_RETRY = 3
  private val DEFAULT_TUBE_SENDER_SERVER = "tl-tdbank-tdmanger.tencent-distribute.com"
  private val DEFAULT_TUBE_SENDER_PORT = 8099
  private val bid = conf.get("spark.sql.audit.tdbank.bid")
  private val tid = conf.get("spark.sql.audit.tdbank.tid")
  private val timeout = conf.getLong("spark.sql.audit.tdbank.timeout",
    DEFAULT_TUBE_SENDER_TIMEOUT_MS)

  def start(): Unit = {
    val tdManagerIp = conf.get("spark.sql.audit.tdbank.tdManagerIp",
      DEFAULT_TUBE_SENDER_SERVER)
    val tdManagerPort = conf.getInt("spark.sql.audit.tdbank.tdManagerPort",
      DEFAULT_TUBE_SENDER_PORT)
    val busConf = new BusClientConfig(InetAddress.getLocalHost.getHostAddress, true,
      tdManagerIp, tdManagerPort, bid, "all")
    busConf.setEnableSaveTdmVIps(false)
    sender = new DefaultMessageSender(busConf)
  }

  // send message to TDBank synchronize
  def sendMessageSync(messages: List[Array[Byte]]): Unit = {
    val maxRetryTimes = conf.getInt("spark.sql.audit.tdbank.sender.maxRetryTimes",
      DEFAULT_TUBE_SENDER_RETRY)
    var success = false
    var retryTimes = 0
    while (!success && retryTimes < maxRetryTimes) {
      retryTimes += 1
      val tubeMessageId = UUID.randomUUID.toString
      val sendResult = sender.sendMessage(messages.asJava, bid, tid, 0,
        tubeMessageId, timeout, TimeUnit.MILLISECONDS)
      if (sendResult == SendResult.OK) {
        success = true
        logInfo(s"Success to send tube message $tubeMessageId" +
          s" to TDBank with bid[$bid], tid[$tid]")
      } else {
        logWarning(s"Get error when sending tube message to TDBank with result code: $sendResult")
      }
    }
  }

  def close(): Unit = {
    if (sender != null) {
      sender.close()
      sender = null
    }
  }
}
