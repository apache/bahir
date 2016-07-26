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
import scala.concurrent.Future

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.streaming.mqtt.MQTTTestUtils

class MQTTStreamSuite extends SparkFunSuite with SharedSparkContext with BeforeAndAfter {

  private var mqttTestUtils: MQTTTestUtils = _
  private val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/mqtt-test/")

  before {
    mqttTestUtils = new MQTTTestUtils
    mqttTestUtils.setup()
    tempDir.mkdirs()
    tempDir.deleteOnExit()
  }

  after {
    mqttTestUtils.teardown()
    tempDir.delete()
  }

  test("basic usage") {
    val sendMessage = "MQTT is a message queue."

    mqttTestUtils.publishData("test", sendMessage)
    val sqlContext: SQLContext = new SQLContext(sc)
    val tmpDir: String = tempDir.getAbsolutePath
    sqlContext.setConf("spark.sql.streaming.checkpointLocation", tmpDir)
    val dataFrame: DataFrame =
    sqlContext.readStream.format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamProvider")
      .load("tcp://" + mqttTestUtils.brokerUri)
    import sqlContext.implicits._
    dataFrame.as[(String, Timestamp)].writeStream.format("parquet").start(s"$tmpDir/t.parquet")
      .awaitTermination(5000)

    val asList =
      sqlContext.read.schema(MQTTStream.SCHEMA_DEFAULT)
        .parquet(s"$tmpDir/t.parquet").as[(String, Timestamp)].map(_._1)
        .filter(_.trim.nonEmpty).collectAsList().asScala
    assert(asList.size == 1)
    assert(asList.head == sendMessage)
  }

}
