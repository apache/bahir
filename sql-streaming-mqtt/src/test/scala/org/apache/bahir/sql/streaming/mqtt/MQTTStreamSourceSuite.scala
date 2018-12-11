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
import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.scalatest.time
import org.scalatest.time.Span

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}

import org.apache.bahir.utils.FileHelper

class MQTTStreamSourceSuite extends SparkFunSuite
    with Eventually with SharedSparkContext with BeforeAndAfter {

  protected var mqttTestUtils: MQTTTestUtils = _
  protected val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/mqtt-test/")

  before {
    tempDir.mkdirs()
    if (!tempDir.exists()) {
      throw new IllegalStateException("Unable to create temp directories.")
    }
    tempDir.deleteOnExit()
    mqttTestUtils = new MQTTTestUtils(tempDir)
    mqttTestUtils.setup()
  }

  after {
    mqttTestUtils.teardown()
    FileHelper.deleteFileQuietly(tempDir)
  }

  protected val tmpDir: String = tempDir.getAbsolutePath

  protected def writeStreamResults(sqlContext: SQLContext, dataFrame: DataFrame): StreamingQuery = {
    import sqlContext.implicits._
    val query: StreamingQuery = dataFrame.selectExpr("CAST(payload AS STRING)").as[String]
      .writeStream.format("parquet").start(s"$tmpDir/t.parquet")
    while (!query.status.isTriggerActive) {
      Thread.sleep(20)
    }
    query
  }

  protected def readBackStreamingResults(sqlContext: SQLContext): mutable.Buffer[String] = {
    import sqlContext.implicits._
    val asList =
      sqlContext.read
        .parquet(s"$tmpDir/t.parquet").as[String]
        .collectAsList().asScala
    asList
  }

  protected def createStreamingDataFrame(dir: String = tmpDir,
      filePersistence: Boolean = false): (SQLContext, DataFrame) = {

    val sqlContext: SQLContext = SparkSession.builder()
      .getOrCreate().sqlContext

    sqlContext.setConf("spark.sql.streaming.checkpointLocation", tmpDir)

    val ds: DataStreamReader =
      sqlContext.readStream.format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
        .option("topic", "test").option("clientId", "clientId").option("connectionTimeout", "120")
        .option("keepAlive", "1200").option("maxInflight", "120").option("autoReconnect", "false")
        .option("cleanSession", "true").option("QoS", "2")

    val dataFrame = if (!filePersistence) {
      ds.option("persistence", "memory").load("tcp://" + mqttTestUtils.brokerUri)
    } else {
      ds.option("persistence", "file").option("localStorage", tmpDir)
        .load("tcp://" + mqttTestUtils.brokerUri)
    }
    (sqlContext, dataFrame)
  }

}

class BasicMQTTSourceSuite extends MQTTStreamSourceSuite {

  test("basic usage") {

    val sendMessage = "MQTT is a message queue."

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataFrame()

    val query = writeStreamResults(sqlContext, dataFrame)
    mqttTestUtils.publishData("test", sendMessage)
    query.processAllAvailable()
    query.awaitTermination(10000)

    val resultBuffer: mutable.Buffer[String] = readBackStreamingResults(sqlContext)

    assert(resultBuffer.size == 1)
    assert(resultBuffer.head == sendMessage)
  }

  test("Send and receive 50 messages.") {

    val sendMessage = "MQTT is a message queue."

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataFrame()

    val q = writeStreamResults(sqlContext, dataFrame)

    mqttTestUtils.publishData("test", sendMessage, 50)
    q.processAllAvailable()
    q.awaitTermination(10000)

    val resultBuffer: mutable.Buffer[String] = readBackStreamingResults(sqlContext)

    assert(resultBuffer.size == 50)
    assert(resultBuffer.head == sendMessage)
  }

  test("messages persisted in store") {
    val sqlContext: SQLContext = SparkSession.builder().getOrCreate().sqlContext
    val source = new MQTTStreamSource(
      DataSourceOptions.empty(), "tcp://" + mqttTestUtils.brokerUri, new MemoryPersistence(),
      "test", "clientId", new MqttConnectOptions(), 2
    )
    val payload = "MQTT is a message queue."
    mqttTestUtils.publishData("test", payload)
    eventually(timeout(Span(5, time.Seconds)), interval(Span(500, time.Millis))) {
      val message = source.store.retrieve(0).asInstanceOf[Object]
      assert(message != null)
    }
    // Clear in-memory cache to simulate recovery.
    source.messages.clear()
    source.setOffsetRange(Optional.empty(), Optional.empty())
    var message: InternalRow = null
    for (f <- source.planInputPartitions().asScala) {
      val dataReader = f.createPartitionReader()
      if (dataReader.next()) {
        message = dataReader.get()
      }
    }
    source.commit(source.getCurrentOffset)
    assert(payload == new String(message.getBinary(2), "UTF-8"))
  }

  test("no server up") {
    val provider = new MQTTStreamSourceProvider
    val sqlContext: SQLContext = SparkSession.builder().getOrCreate().sqlContext
    val parameters = new DataSourceOptions(Map("brokerUrl" ->
      "tcp://localhost:1881", "topic" -> "test", "localStorage" -> tmpDir).asJava)
    intercept[MqttException] {
      provider.createMicroBatchReader(Optional.empty(), tempDir.toString, parameters)
    }
  }

  test("params not provided.") {
    val provider = new MQTTStreamSourceProvider
    val parameters = new DataSourceOptions(Map("brokerUrl" -> mqttTestUtils.brokerUri,
      "localStorage" -> tmpDir).asJava)
    intercept[IllegalArgumentException] {
      provider.createMicroBatchReader(Optional.empty(), tempDir.toString, parameters)
    }
    intercept[IllegalArgumentException] {
      provider.createMicroBatchReader(Optional.empty(), tempDir.toString, DataSourceOptions.empty())
    }
  }

}

class StressTestMQTTSource extends MQTTStreamSourceSuite {

  // Run with -Xmx1024m
  test("Send and receive messages of size 100MB.") {

    val freeMemory: Long = Runtime.getRuntime.freeMemory()

    log.info(s"Available memory before test run is ${freeMemory / (1024 * 1024)}MB.")

    val noOfMsgs: Int = (100 * 1024 * 1024) / (500 * 1024) // 204

    val messageBuilder = new StringBuilder()
    for (i <- 0 until (500 * 1024)) yield messageBuilder.append(((i % 26) + 65).toChar)
    val sendMessage = messageBuilder.toString() // each message is 50 KB

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataFrame()

    val query = writeStreamResults(sqlContext, dataFrame)
    mqttTestUtils.publishData("test", sendMessage, noOfMsgs )
    query.processAllAvailable()
    query.awaitTermination(25000)

    val messageCount =
      sqlContext.read
        .parquet(s"$tmpDir/t.parquet")
        .count()
    assert(messageCount == noOfMsgs)
  }
}
