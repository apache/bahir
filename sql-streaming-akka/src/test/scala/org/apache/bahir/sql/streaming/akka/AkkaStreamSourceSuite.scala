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

package org.apache.bahir.sql.streaming.akka

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.execution.streaming.LongOffset

import org.apache.bahir.utils.BahirUtils

class AkkaStreamSourceSuite extends SparkFunSuite with BeforeAndAfter {

  protected var akkaTestUtils: AkkaTestUtils = _
  protected val tempDir: File =
    new File(System.getProperty("java.io.tmpdir") + "/spark-akka-persistence")

  private val conf = new SparkConf().setMaster("local[4]").setAppName("AkkaStreamSourceSuite")
  protected val spark = SparkSession.builder().config(conf).getOrCreate()

  before {
    tempDir.mkdirs()
    akkaTestUtils = new AkkaTestUtils
    akkaTestUtils.setup()
  }

  after {
    Persistence.close()
    BahirUtils.recursiveDeleteDir(tempDir)
  }

  protected val tmpDir: String = tempDir.getAbsolutePath

  protected def createStreamingDataframe(dir: String = tmpDir): (SQLContext, DataFrame) = {

    val sqlContext: SQLContext = spark.sqlContext

    sqlContext.setConf("spark.sql.streaming.checkpointLocation", dir + "/checkpoint")

    val dataFrame: DataFrame =
      sqlContext.readStream.format("org.apache.bahir.sql.streaming.akka.AkkaStreamSourceProvider")
        .option("urlOfPublisher", akkaTestUtils.getFeederActorUri())
        .option("persistenceDirPath", dir + "/persistence").load()
    (sqlContext, dataFrame)
  }
}

class BasicAkkaSourceSuite extends AkkaStreamSourceSuite {

  private def writeStreamResults(sqlContext: SQLContext, dataFrame: DataFrame,
                                 waitDuration: Long): Boolean = {
    import sqlContext.implicits._
    dataFrame.as[(String, Timestamp)].writeStream.format("parquet")
      .start(s"$tmpDir/parquet/t.parquet").awaitTermination(waitDuration)
  }

  private def readBackSreamingResults(sqlContext: SQLContext): mutable.Buffer[String] = {
    import sqlContext.implicits._
    val asList =
      sqlContext.read.schema(AkkaStreamConstants.SCHEMA_DEFAULT)
      .parquet(s"$tmpDir/parquet/t.parquet").as[(String, Timestamp)].map(_._1)
      .collectAsList().asScala
    asList
  }

  test("basic usage") {
    val message = "Akka is a reactive framework"

    akkaTestUtils.setMessage(message)
    akkaTestUtils.setCountOfMessages(1)

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataframe()

    writeStreamResults(sqlContext, dataFrame, 10000)

    val resultBuffer: mutable.Buffer[String] = readBackSreamingResults(sqlContext)

    assert(resultBuffer.size === 1)
    assert(resultBuffer.head === message)
  }

  test("Send and receive 100 messages.") {
    val message = "Akka is a reactive framework"

    akkaTestUtils.setMessage(message)
    akkaTestUtils.setCountOfMessages(100)

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataframe()

    writeStreamResults(sqlContext, dataFrame, 10000)

    val resultBuffer: mutable.Buffer[String] = readBackSreamingResults(sqlContext)

    assert(resultBuffer.size === 100)
    assert(resultBuffer.head === message)
  }

  test("params not provided") {
    val persistenceDirPath = tempDir.getAbsolutePath + "/persistence"

    val provider = new AkkaStreamSourceProvider
    val sqlContext: SQLContext = spark.sqlContext

    val parameters = Map("persistenceDirPath" -> persistenceDirPath)

    intercept[IllegalArgumentException] {
      provider.createSource(sqlContext, "", None, "", parameters)
    }
  }

  test("Recovering offset from the last processed offset") {
    val persistenceDirPath = tempDir.getAbsolutePath + "/persistence"
    val message = "Akka is a reactive framework"

    akkaTestUtils.setMessage(message)
    akkaTestUtils.setCountOfMessages(100)

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataframe()

    writeStreamResults(sqlContext, dataFrame, 10000)

    val provider = new AkkaStreamSourceProvider
    val parameters = Map("urlOfPublisher" -> akkaTestUtils.getFeederActorUri(),
      "persistenceDirPath" -> persistenceDirPath)

    val offset: Long = provider.createSource(sqlContext, "", None, "", parameters)
      .getOffset.get.asInstanceOf[LongOffset].offset
    assert(offset === 100L)
  }
}

class StressTestAkkaSource extends AkkaStreamSourceSuite {

  // Run with -Xmx1024m
  // Default allowed payload size sent to an akka actor is 128000 bytes.
  test("Send & Receive messages of size 128000 bytes.") {

    val freeMemory: Long = Runtime.getRuntime.freeMemory()

    log.info(s"Available memory before test run is ${freeMemory / (1024 * 1024)}MB.")

    val noOfMsgs = 124 * 1024

    val messageBuilder = new mutable.StringBuilder()
    for (i <- 0 until noOfMsgs) yield messageBuilder.append(((i % 26) + 65).toChar)

    val message = messageBuilder.toString()

    akkaTestUtils.setMessage(message)
    akkaTestUtils.setCountOfMessages(1)

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataframe()

    import sqlContext.implicits._

    dataFrame.as[(String, Timestamp)].writeStream
      .format("parquet")
      .start(s"$tmpDir/parquet/t.parquet")
      .awaitTermination(25000)

    val outputMessage =
      sqlContext.read.schema(AkkaStreamConstants.SCHEMA_DEFAULT)
        .parquet(s"$tmpDir/parquet/t.parquet").as[(String, Timestamp)]
        .map(_._1).head()

    assert(outputMessage === message)
  }
}
