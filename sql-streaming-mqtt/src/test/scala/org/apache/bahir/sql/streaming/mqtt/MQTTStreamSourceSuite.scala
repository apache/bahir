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
import java.sql.Timestamp

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future

import org.eclipse.paho.client.mqttv3.MqttException
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.LongOffset

import org.apache.bahir.utils.BahirUtils


class MQTTStreamSourceSuite extends SparkFunSuite with SharedSparkContext with BeforeAndAfter {

  protected var mqttTestUtils: MQTTTestUtils = _
  protected val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/mqtt-test/")

  before {
    mqttTestUtils = new MQTTTestUtils(tempDir)
    mqttTestUtils.setup()
    tempDir.mkdirs()
  }

  after {
    mqttTestUtils.teardown()
    BahirUtils.recursiveDeleteDir(tempDir)
  }

  protected val tmpDir: String = tempDir.getAbsolutePath

  protected def createStreamingDataframe(dir: String = tmpDir): (SQLContext, DataFrame) = {

    val sqlContext: SQLContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.streaming.checkpointLocation", tmpDir)

    val dataFrame: DataFrame =
      sqlContext.readStream.format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
        .option("topic", "test").option("localStorage", dir).option("clientId", "clientId")
        .option("QoS", "2").load("tcp://" + mqttTestUtils.brokerUri)
    (sqlContext, dataFrame)
  }

}

class BasicMQTTSourceSuite extends MQTTStreamSourceSuite {

  private def writeStreamResults(sqlContext: SQLContext,
      dataFrame: DataFrame, waitDuration: Long): Boolean = {
    import sqlContext.implicits._
    dataFrame.as[(String, Timestamp)].writeStream.format("parquet").start(s"$tmpDir/t.parquet")
      .awaitTermination(waitDuration)
  }

  private def readBackStreamingResults(sqlContext: SQLContext): mutable.Buffer[String] = {
    import sqlContext.implicits._
    val asList =
      sqlContext.read.schema(MQTTStreamConstants.SCHEMA_DEFAULT)
        .parquet(s"$tmpDir/t.parquet").as[(String, Timestamp)].map(_._1)
        .collectAsList().asScala
    asList
  }

  test("basic usage") {

    val sendMessage = "MQTT is a message queue."

    mqttTestUtils.publishData("test", sendMessage)

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataframe()

    writeStreamResults(sqlContext, dataFrame, 5000)

    val resultBuffer: mutable.Buffer[String] = readBackStreamingResults(sqlContext)

    assert(resultBuffer.size == 1)
    assert(resultBuffer.head == sendMessage)
  }

  // TODO: reinstate this test after fixing BAHIR-83
  ignore("Send and receive 100 messages.") {

    val sendMessage = "MQTT is a message queue."

    import scala.concurrent.ExecutionContext.Implicits.global

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataframe()

    Future {
      Thread.sleep(2000)
      mqttTestUtils.publishData("test", sendMessage, 100)
    }

    writeStreamResults(sqlContext, dataFrame, 10000)

    val resultBuffer: mutable.Buffer[String] = readBackStreamingResults(sqlContext)

    assert(resultBuffer.size == 100)
    assert(resultBuffer.head == sendMessage)
  }

  test("no server up") {
    val provider = new MQTTStreamSourceProvider
    val sqlContext: SQLContext = new SQLContext(sc)
    val parameters = Map("brokerUrl" -> "tcp://localhost:1883", "topic" -> "test",
      "localStorage" -> tmpDir)
    intercept[MqttException] {
      provider.createSource(sqlContext, "", None, "", parameters)
    }
  }

  test("params not provided.") {
    val provider = new MQTTStreamSourceProvider
    val sqlContext: SQLContext = new SQLContext(sc)
    val parameters = Map("brokerUrl" -> mqttTestUtils.brokerUri,
      "localStorage" -> tmpDir)
    intercept[IllegalArgumentException] {
      provider.createSource(sqlContext, "", None, "", parameters)
    }
    intercept[IllegalArgumentException] {
      provider.createSource(sqlContext, "", None, "", Map())
    }
  }

  // TODO: reinstate this test after fixing BAHIR-83
  ignore("Recovering offset from the last processed offset.") {
    val sendMessage = "MQTT is a message queue."

    import scala.concurrent.ExecutionContext.Implicits.global

    val (sqlContext: SQLContext, dataFrame: DataFrame) =
      createStreamingDataframe()

    Future {
      Thread.sleep(2000)
      mqttTestUtils.publishData("test", sendMessage, 100)
    }

    writeStreamResults(sqlContext, dataFrame, 10000)
    // On restarting the source with same params, it should begin from the offset - the
    // previously running stream left off.
    val provider = new MQTTStreamSourceProvider
    val parameters = Map("brokerUrl" -> ("tcp://" + mqttTestUtils.brokerUri), "topic" -> "test",
      "localStorage" -> tmpDir, "clientId" -> "clientId", "QoS" -> "2")
    val offset: Long = provider.createSource(sqlContext, "", None, "", parameters)
      .getOffset.get.asInstanceOf[LongOffset].offset
    assert(offset == 100L)
  }

}

class StressTestMQTTSource extends MQTTStreamSourceSuite {

  // Run with -Xmx1024m
  ignore("Send and receive messages of size 250MB.") {

    val freeMemory: Long = Runtime.getRuntime.freeMemory()

    log.info(s"Available memory before test run is ${freeMemory / (1024 * 1024)}MB.")

    val noOfMsgs = (250 * 1024 * 1024) / (500 * 1024) // 512

    val messageBuilder = new StringBuilder()
    for (i <- 0 until (500 * 1024)) yield messageBuilder.append(((i % 26) + 65).toChar)
    val sendMessage = messageBuilder.toString() // each message is 50 KB

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataframe()

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      Thread.sleep(2000)
      mqttTestUtils.publishData("test", sendMessage, noOfMsgs.toInt)
    }

    import sqlContext.implicits._

    dataFrame.as[(String, Timestamp)].writeStream
      .format("parquet")
      .start(s"$tmpDir/t.parquet")
      .awaitTermination(25000)

    val messageCount =
      sqlContext.read.schema(MQTTStreamConstants.SCHEMA_DEFAULT)
        .parquet(s"$tmpDir/t.parquet").as[(String, Timestamp)].map(_._1)
        .count()
    assert(messageCount == noOfMsgs)
  }
}
