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

package org.apache.bahir.sql.streaming.mqtt

import java.io.File
import java.net.ConnectException
import java.util

import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttException
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future

import org.apache.spark.{SharedSparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

import org.apache.bahir.utils.FileHelper


class MQTTStreamSinkSuite(_ssl: Boolean) extends SparkFunSuite
    with SharedSparkContext with BeforeAndAfter {
  protected var mqttTestUtils: MQTTTestUtils = _
  protected val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/mqtt-test/")
  protected val messages = new mutable.HashMap[Int, String]
  protected var testClient: MqttClient = _

  before {
    SparkEnv.get.conf.set("spark.mqtt.client.connect.attempts", "1")
    mqttTestUtils = new MQTTTestUtils(tempDir, ssl = _ssl)
    mqttTestUtils.setup()
    tempDir.mkdirs()
    messages.clear()
    testClient = mqttTestUtils.subscribeData("test", messages)
  }

  after {
    CachedMQTTClient.clear()
    testClient.disconnectForcibly()
    testClient.close()
    mqttTestUtils.teardown()
    FileHelper.deleteFileQuietly(tempDir)
  }

  protected def createContextAndDF(messages: String*): (SQLContext, DataFrame) = {
    val sqlContext: SQLContext = SparkSession.builder().getOrCreate().sqlContext
    sqlContext.setConf("spark.sql.streaming.checkpointLocation", tempDir.getAbsolutePath)
    import sqlContext.sparkSession.implicits._
    val stream = new MemoryStream[String](1, sqlContext)
    stream.addData(messages.toSeq)
    (sqlContext, stream.toDF())
  }

  protected def sendToMQTT(dataFrame: DataFrame): StreamingQuery = {
    val protocol = if (_ssl) "ssl" else "tcp"
    val writer = dataFrame.writeStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSinkProvider")
      .option("topic", "test").option("localStorage", tempDir.getAbsolutePath)
      .option("clientId", "clientId").option("QoS", "2")
    if (_ssl) {
      writer.option("ssl.trust.store", mqttTestUtils.clientTrustStore.getAbsolutePath)
        .option("ssl.trust.store.type", "JKS")
        .option("ssl.trust.store.password", mqttTestUtils.clientTrustStorePassword)
    }
    writer.start(protocol + "://" + mqttTestUtils.brokerUri)
  }
}

class BasicMQTTSinkSuite extends MQTTStreamSinkSuite(false) {
  test("broker down") {
    SparkSession.setActiveSession(SparkSession.builder().getOrCreate())
    val provider = new MQTTStreamSinkProvider
    val parameters = Map(
      "brokerUrl" -> "tcp://localhost:1883",
      "topic" -> "test",
      "localStorage" -> tempDir.getAbsoluteFile.toString
    )
    val thrown: Exception = intercept[MqttException] {
      provider.createStreamWriter(
        "query1", null, OutputMode.Complete(), new DataSourceOptions(parameters.asJava)
      ).createWriterFactory().createDataWriter(1, 1, 1).write(null)
    }
    assert(thrown.getCause.isInstanceOf[ConnectException])
  }

  test("basic usage") {
    val msg1 = "Hello, World!"
    val msg2 = "MQTT is a message queue."
    val (_, dataFrame) = createContextAndDF(msg1, msg2)

    sendToMQTT(dataFrame).awaitTermination(5000)

    assert(Set(msg1, msg2).equals(messages.values.toSet))
  }

  test("send and receive 100 messages") {
    val msg = List.tabulate(100)(n => "Hello, World!" + n)
    val (_, dataFrame) = createContextAndDF(msg: _*)

    sendToMQTT(dataFrame).awaitTermination(5000)

    assert(Set(msg: _*).equals(messages.values.toSet))
  }

  test("missing configuration") {
    val provider = new MQTTStreamSinkProvider
    val parameters = Map(
      "brokerUrl" -> "tcp://localhost:1883",
      "localStorage" -> tempDir.getAbsoluteFile.toString
    )
    intercept[IllegalArgumentException] {
      provider.createStreamWriter(
        "query1", null, OutputMode.Complete(), new DataSourceOptions(parameters.asJava)
      ).createWriterFactory().createDataWriter(1, 1, 1).write(null)
    }
    intercept[IllegalArgumentException] {
      provider.createStreamWriter(
        "query1", null, OutputMode.Complete(),
        new DataSourceOptions(new util.HashMap[String, String])
      ).createWriterFactory().createDataWriter(1, 1, 1).write(null)
    }
  }
}

class MQTTSSLSinkSuite extends MQTTStreamSinkSuite(true) {
  test("verify SSL connectivity") {
    val msg1 = "Hello, World!"
    val msg2 = "MQTT is a message queue."
    val (_, dataFrame) = createContextAndDF(msg1, msg2)

    sendToMQTT(dataFrame).awaitTermination(5000)

    assert(Set(msg1, msg2).equals(messages.values.toSet))
  }
}

class StressTestMQTTSink extends MQTTStreamSinkSuite(false) {
  // run with -Xmx1024m
  test("Send and receive messages of size 100MB.") {
    val freeMemory: Long = Runtime.getRuntime.freeMemory()
    log.info(s"Available memory before test run is ${freeMemory / (1024 * 1024)}MB.")
    val noOfMsgs: Int = 200
    val noOfBatches: Int = 5

    val messageBuilder = new StringBuilder()
    for (i <- 0 until (500 * 1024)) yield messageBuilder.append(((i % 26) + 65).toChar)
    val message = messageBuilder.toString()
    val (_, dataFrame) = createContextAndDF(
      // each message is 50 KB
      Array.fill(noOfMsgs / noOfBatches)(message): _*
    )

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      for (_ <- 0 until noOfBatches.toInt) {
        sendToMQTT(dataFrame)
      }
    }
    def waitForMessages(): Boolean = {
      messages.size == noOfMsgs
    }

    mqttTestUtils.sleepUntil(waitForMessages(), 60000)

    assert(messages.size == noOfMsgs)
    assert(messageBuilder.toString().equals(messages.head._2))
  }
}
