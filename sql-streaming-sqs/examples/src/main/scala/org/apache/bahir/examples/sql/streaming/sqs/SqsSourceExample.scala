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

package org.apache.bahir.examples.sql.streaming.sqs

import scala.util.Random

import org.apache.spark.sql.SparkSession

 /**
  * Example to read files from S3 using SQS Source and write results to Memory Sink
  *
  * Usage: SqsSourceExample <Sample Record Path to infer schema> <SQS Queue URL> <File Format>
  */

object SqsSourceExample {

  def main(args: Array[String]) {

    val randomName = Random.alphanumeric.take(6).mkString("")
    val pathName = "path_" + randomName
    val queryName = "query_" + randomName
    val checkpointDir = s"/checkpoints/$pathName"
    val schemaPathString = args(0)

    val spark = SparkSession.builder().appName("SqsExample").getOrCreate()

    val schema = spark.read.json(schemaPathString).schema

    val queueUrl = args(1)

    val fileFormat = args(2)

    val inputDf = spark
      .readStream
      .format("s3-sqs")
      .schema(schema)
      .option("sqsUrl", queueUrl)
      .option("fileFormat", fileFormat)
      .option("sqsFetchIntervalSeconds", "2")
      .option("sqsLongPollingWaitTimeSeconds", "5")
      .option("maxFilesPerTrigger", "50")
      .option("ignoreFileDeletion", "true")
      .load()

    val query = inputDf
      .writeStream
      .queryName(queryName)
      .format("memory")
      .option("checkpointLocation", checkpointDir)
      .start()

    query.awaitTermination()
  }
}




