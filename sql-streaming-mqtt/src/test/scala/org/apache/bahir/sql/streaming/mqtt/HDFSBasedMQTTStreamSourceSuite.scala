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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}

import org.apache.bahir.utils.BahirUtils

class HDFSBasedMQTTStreamSourceSuite
    extends SparkFunSuite
    with SharedSparkContext
    with BeforeAndAfter {

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
    // BahirUtils.recursiveDeleteDir(tempDir)
  }

  protected val tmpDir: String = tempDir.getAbsolutePath

  protected def writeStreamResults(sqlContext: SQLContext, dataFrame: DataFrame): StreamingQuery = {
    import sqlContext.implicits._
    dataFrame.printSchema()
    val query: StreamingQuery = dataFrame.selectExpr("CAST(payload AS STRING)").as[String]
      .writeStream.format("csv").start(s"$tempDir/t.csv")
    while (!query.status.isTriggerActive) {
      // scalastyle:off
      println("waiting for 20 ms")
      // scalastyle:on
      Thread.sleep(200)
    }
    query
  }

  protected def readBackStreamingResults(sqlContext: SQLContext): mutable.Buffer[String] = {
    import sqlContext.implicits._
    val asList =
      sqlContext.read
        .csv(s"$tmpDir/t.csv").as[String]
        .collectAsList().asScala
    asList
  }

  protected def createStreamingDataFrame(dir: String = tmpDir): (SQLContext, DataFrame) = {

    val sqlContext: SQLContext = SparkSession.builder()
      .getOrCreate().sqlContext

    sqlContext.setConf("spark.sql.streaming.checkpointLocation", tmpDir)

    val ds: DataStreamReader =
      sqlContext.readStream.format("org.apache.bahir.sql.streaming.mqtt.HDFSMQTTSourceProvider")
        .option("topic", "test").option("clientId", "clientId").option("connectionTimeout", "120")
        .option("keepAlive", "1200").option("autoReconnect", "false")
        .option("cleanSession", "true").option("QoS", "2")
    val dataFrame = ds.load("tcp://" + mqttTestUtils.brokerUri)
    (sqlContext, dataFrame)
  }
}

class BasicHDFSBasedMQTTSourceSuite extends HDFSBasedMQTTStreamSourceSuite {

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
}

