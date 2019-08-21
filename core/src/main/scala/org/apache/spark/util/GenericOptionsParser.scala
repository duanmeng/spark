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

package org.apache.spark.util

import org.apache.commons.cli.{CommandLine, CommandLineParser, GnuParser, HelpFormatter, Option, OptionBuilder, Options, ParseException}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging


/**
 * Created by taguswang
 * 14-4-30.
 */
class GenericOptionsParser(conf: SparkConf, args: Array[String])
  extends Logging {

  var commandLine: CommandLine = _

  def buildGeneralOptions(opts: Options): Options = {
    OptionBuilder.withArgName("property=value")
    OptionBuilder.hasArg()
    OptionBuilder.withDescription("use value for given property")
    val property: Option = OptionBuilder.create('D')
    opts.addOption(property)
    opts
  }

  def parseGeneralOptions(): Array[String] = {
    val parser: CommandLineParser = new GnuParser()
    var opts = new Options()
    try {
      opts = buildGeneralOptions(opts)
      commandLine = parser.parse(opts, args, true)

      if (commandLine.hasOption('D')) {
        val property: Array[String] = commandLine.getOptionValues('D')
        for (prop <- property) {
          val keyval: Array[String] = prop.split("=", 2)
          if (keyval.length == 2) {
            logInfo(keyval(0) + "=" + keyval(1))
            conf.set(keyval(0), keyval(1))
          }
        }
      }

      commandLine.getArgs
    } catch {
      case e: ParseException =>
        logWarning("options parsing failed: " + e.getMessage())

        val formatter: HelpFormatter = new HelpFormatter()
        formatter.printHelp("general options are: ", opts)
        args
    }
  }

  def getRemainingArgs(): Array[String] = {
    commandLine.getArgs
  }

}
