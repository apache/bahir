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

<<<<<<< HEAD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.security.Groups
=======
>>>>>>> code reuse and add unit test.
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql._
<<<<<<< HEAD
import org.apache.spark.sql.mqtt.HdfsBasedMQTTStreamSource
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}

import org.apache.bahir.utils.{FileHelper}
=======
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}

import org.apache.bahir.utils.BahirUtils
>>>>>>> code reuse and add unit test.

class HDFSBasedMQTTStreamSourceSuite
    extends SparkFunSuite
    with SharedSparkContext
    with BeforeAndAfter {

  protected var mqttTestUtils: MQTTTestUtils = _
  protected val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/mqtt-test/")
<<<<<<< HEAD
  private var hadoop: MiniDFSCluster = _
=======
>>>>>>> code reuse and add unit test.

  before {
    tempDir.mkdirs()
    if (!tempDir.exists()) {
      throw new IllegalStateException("Unable to create temp directories.")
    }
    tempDir.deleteOnExit()
    mqttTestUtils = new MQTTTestUtils(tempDir)
    mqttTestUtils.setup()
<<<<<<< HEAD
    hadoop = HDFSTestUtils.prepareHadoop()
=======
>>>>>>> code reuse and add unit test.
  }

  after {
    mqttTestUtils.teardown()
<<<<<<< HEAD
    HDFSTestUtils.shutdownHadoop()
    FileHelper.deleteFileQuietly(tempDir)
=======
    // BahirUtils.recursiveDeleteDir(tempDir)
>>>>>>> code reuse and add unit test.
  }

  protected val tmpDir: String = tempDir.getAbsolutePath

  protected def writeStreamResults(sqlContext: SQLContext, dataFrame: DataFrame): StreamingQuery = {
    import sqlContext.implicits._
    dataFrame.printSchema()
    val query: StreamingQuery = dataFrame.selectExpr("CAST(payload AS STRING)").as[String]
      .writeStream.format("csv").start(s"$tempDir/t.csv")
    while (!query.status.isTriggerActive) {
<<<<<<< HEAD
      Thread.sleep(20)
=======
      // scalastyle:off
      println("waiting for 20 ms")
      // scalastyle:on
      Thread.sleep(200)
>>>>>>> code reuse and add unit test.
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

<<<<<<< HEAD
    sqlContext.setConf("spark.sql.streaming.checkpointLocation",
      s"hdfs://localhost:${hadoop.getNameNodePort}/testCheckpoint")

    val ds: DataStreamReader =
      sqlContext.readStream.format("org.apache.spark.sql.mqtt.HDFSMQTTSourceProvider")
=======
    sqlContext.setConf("spark.sql.streaming.checkpointLocation", tmpDir)

    val ds: DataStreamReader =
      sqlContext.readStream.format("org.apache.bahir.sql.streaming.mqtt.HDFSMQTTSourceProvider")
>>>>>>> code reuse and add unit test.
        .option("topic", "test").option("clientId", "clientId").option("connectionTimeout", "120")
        .option("keepAlive", "1200").option("autoReconnect", "false")
        .option("cleanSession", "true").option("QoS", "2")
    val dataFrame = ds.load("tcp://" + mqttTestUtils.brokerUri)
    (sqlContext, dataFrame)
  }
}

<<<<<<< HEAD
object HDFSTestUtils {

  private var hadoop: MiniDFSCluster = _

  def prepareHadoop(): MiniDFSCluster = {
    if (hadoop != null) {
      hadoop
    } else {
      val baseDir = new File(System.getProperty("java.io.tmpdir") + "/hadoop").getAbsoluteFile
      System.setProperty("HADOOP_USER_NAME", "test")
      val conf = new Configuration
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
      conf.setBoolean("dfs.namenode.acls.enabled", true)
      conf.setBoolean("dfs.permissions", true)
      Groups.getUserToGroupsMappingService(conf)
      val builder = new MiniDFSCluster.Builder(conf)
      hadoop = builder.build
      conf.set("fs.defaultFS", "hdfs://localhost:" + hadoop.getNameNodePort + "/")
      HdfsBasedMQTTStreamSource.hadoopConfig = conf
      hadoop
    }
  }

  def shutdownHadoop(): Unit = {
    if (null != hadoop) {
      hadoop.shutdown(true)
    }
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
  // TODO, more unit tests
}

