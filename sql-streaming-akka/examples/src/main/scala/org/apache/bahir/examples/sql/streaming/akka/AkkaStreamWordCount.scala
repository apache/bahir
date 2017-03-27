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

package org.apache.bahir.examples.sql.streaming.akka

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from Akka Feeder Actor system.
 *
 * Usage: AkkaStreamWordCount <urlOfPublisher>
 * <urlOfPublisher> provides the uri of the publisher or feeder actor that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, a Feeder Actor System should be up and running.
 *
 */
object AkkaStreamWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: AkkaStreamWordCount <urlOfPublisher>") // scalastyle:off println
      System.exit(1)
    }

    val urlOfPublisher = args(0)

    val spark = SparkSession
                .builder()
                .appName("AkkaStreamWordCount")
                .master("local[4]")
                .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection
    // to publisher or feeder actor
    val lines = spark.readStream
                .format("org.apache.bahir.sql.streaming.akka.AkkaStreamSourceProvider")
                .option("urlOfPublisher", urlOfPublisher)
                .load().as[(String, Timestamp)]

    // Split the lines into words
    val words = lines.map(_._1).flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
                .outputMode("complete")
                .format("console")
                .start()

    query.awaitTermination()
  }
}
