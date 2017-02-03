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

// scalastyle:off println
package org.apache.spark.examples.streaming.mqtt

import org.apache.log4j.{Level, Logger}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.mqtt._
import org.apache.spark.SparkConf

/**
 * A simple Mqtt publisher for demonstration purposes, repeatedly publishes
 * Space separated String Message "hello mqtt demo for spark streaming"
 */
object MQTTPublisher {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: MQTTPublisher <MqttBrokerUrl> <topic>")
      System.exit(1)
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val Seq(brokerUrl, topic) = args.toSeq

    var client: MqttClient = null

    try {
      val persistence = new MemoryPersistence()
      client = new MqttClient(brokerUrl, MqttClient.generateClientId(), persistence)

      client.connect()

      val msgtopic = client.getTopic(topic)
      val msgContent = "hello mqtt demo for spark streaming"
      val message = new MqttMessage(msgContent.getBytes("utf-8"))

      while (true) {
        try {
          msgtopic.publish(message)
          println(s"Published data. topic: ${msgtopic.getName()}; Message: $message")
        } catch {
          case e: MqttException if e.getReasonCode == MqttException.REASON_CODE_MAX_INFLIGHT =>
            Thread.sleep(10)
            println("Queue is full, wait for to consume data from the message queue")
        }
      }
    } catch {
      case e: MqttException => println("Exception Caught: " + e)
    } finally {
      if (client != null) {
        client.disconnect()
      }
    }
  }
}

/**
 * A sample wordcount with MQTTInputDStream
 *
 * Usage: MQTTWordCount <MqttbrokerUrl> <topic>
 *
 * To run this example on your local machine, you first need to setup a MQTT broker and publisher,
 * like Mosquitto (http://mosquitto.org/) an easy to use and install open source MQTT Broker.
 * On Mac OS, Mosquitto can be installed with Homebrew `$ brew install mosquitto`.
 * On Ubuntu, Mosquitto can be installed with the command `$ sudo apt-get install mosquitto`.
 *
 * Alternatively, checkout the Eclipse paho project which provides a number of clients and utilities
 * for working with MQTT (http://www.eclipse.org/paho/#getting-started).
 *
 * How to run this example locally:
 *
 * (1) Start a MQTT message broker/server, i.e. Mosquitto:
 *
 *    `$ mosquitto -p 1883`
 *
 * (2) Run the publisher:
 *
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.mqtt.MQTTPublisher tcp://localhost:1883 foo`
 *
 * (3) Run the example:
 *
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.mqtt.MQTTWordCount tcp://localhost:1883 foo`
 */

object MQTTWordCount {

  def main(args: Array[String]) {
    if (args.length < 2) {
      // scalastyle:off println
      System.err.println(
        "Usage: MQTTWordCount <MqttbrokerUrl> <topic>")
      // scalastyle:on println
      System.exit(1)
    }

    val Seq(brokerUrl, topic) = args.toSeq
    val sparkConf = new SparkConf().setAppName("MQTTWordCount")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val lines = MQTTUtils.createStream(ssc, brokerUrl, topic, StorageLevel.MEMORY_ONLY_SER_2)
    val words = lines.flatMap(x => x.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
