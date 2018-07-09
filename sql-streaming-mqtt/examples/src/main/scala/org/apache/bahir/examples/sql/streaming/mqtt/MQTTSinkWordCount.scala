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

package org.apache.bahir.examples.sql.streaming.mqtt

import java.io.File

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.SparkSession

/**
 * Counts words in UTF-8 encoded, '\n' delimited text received from local socket
 * and publishes results on MQTT topic.
 *
 * Usage: MQTTSinkWordCount <port> <brokerUrl> <topic>
 * <port> represents local network port on which program is listening for input.
 * <brokerUrl> and <topic> describe the MQTT server that structured streaming
 * would connect and send data.
 *
 * To run example on your local machine, a MQTT Server should be up and running.
 * Linux users may leverage 'nc -lk <port>' to listen on local port and wait
 * for Spark socket connection.
 */
object MQTTSinkWordCount  {
  def main(args: Array[String]) {
    if (args.length < 2) {
      // scalastyle:off
      System.err.println("Usage: MQTTSinkWordCount <port> <brokerUrl> <topic>")
      // scalastyle:on
      System.exit(1)
    }

    val checkpointDir = System.getProperty("java.io.tmpdir") + "/mqtt-example/"
    // Remove checkpoint directory.
    FileUtils.deleteDirectory(new File(checkpointDir))

    val port = args(0)
    val brokerUrl = args(1)
    val topic = args(2)

    val spark = SparkSession.builder
      .appName("MQTTSinkWordCount").master("local[4]")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from local network socket.
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost").option("port", port)
      .load().select("value").as[String]

    // Split the lines into words.
    val words = lines.flatMap(_.split(" "))

    // Generate running word count.
    val wordCounts = words.groupBy("value").count()

    // Start publishing the counts to MQTT server.
    val query = wordCounts.writeStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSinkProvider")
      .option("checkpointLocation", checkpointDir)
      .outputMode("complete")
      .option("topic", topic)
      .option("localStorage", checkpointDir)
      .start(brokerUrl)

    query.awaitTermination()
  }
}
