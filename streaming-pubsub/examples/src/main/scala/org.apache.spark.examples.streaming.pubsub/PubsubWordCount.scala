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
package org.apache.spark.examples.streaming.pubsub

import scala.collection.JavaConverters._
import scala.util.Random

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.pubsub.Pubsub.Builder
import com.google.api.services.pubsub.model.PublishRequest
import com.google.api.services.pubsub.model.PubsubMessage
import com.google.cloud.hadoop.util.RetryHttpInitializer

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.pubsub.ConnectionUtils
import org.apache.spark.streaming.pubsub.PubsubTestUtils
import org.apache.spark.streaming.pubsub.PubsubUtils
import org.apache.spark.streaming.pubsub.SparkGCPCredentials
import org.apache.spark.streaming.pubsub.SparkPubsubMessage
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf


/**
 * Consumes messages from a Google Cloud Pub/Sub subscription and does wordcount.
 * In this example it use application default credentials, so need to use gcloud
 * client to generate token file before running example
 *
 * Usage: PubsubWordCount <projectId> <subscription>
 *   <projectId> is the name of Google cloud
 *   <subscription> is the subscription to a topic
 *
 * Example:
 *  # use gcloud client generate token file
 *  $ gcloud init
 *  $ gcloud auth application-default login
 *
 *  # run the example
 *  $ bin/run-example \
 *      org.apache.spark.examples.streaming.pubsub.PubsubWordCount project_1 subscription_1
 *
 */
object PubsubWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println(
        """
          |Usage: PubsubWordCount <projectId> <subscription>
          |
          |     <projectId> is the name of Google cloud
          |     <subscription> is the subscription to a topic
          |
        """.stripMargin)
      System.exit(1)
    }

    val Seq(projectId, subscription) = args.toSeq

    val sparkConf = new SparkConf().setAppName("PubsubWordCount")
    val ssc = new StreamingContext(sparkConf, Milliseconds(2000))

    val pubsubStream: ReceiverInputDStream[SparkPubsubMessage] = PubsubUtils.createStream(
      ssc, projectId, None, subscription,
      SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)

    val wordCounts =
      pubsubStream.map(message => (new String(message.getData()), 1)).reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

/**
 * A Pub/Sub publisher for demonstration purposes, publishes message in 10 batches(seconds),
 * you can set the size of messages in each batch by <records-per-sec>,
 * and each message will contains only one word in this list
 * ("google", "cloud", "pubsub", "say", "hello")
 *
 * Usage: PubsubPublisher <projectId> <topic> <records-per-sec>
 *
 *   <stream-projectIdname> is the name of Google cloud
 *   <topic> is the topic of Google cloud Pub/Sub
 *   <records-per-sec> is the rate of records per second to put onto the stream
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.pubsub.PubsubPublisher project_1 topic_1 10`
 */
object PubsubPublisher {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println(
        """
          |Usage: PubsubPublisher <projectId> <topic> <records-per-sec>
          |
          |     <projectId> is the name of Google cloud
          |     <topic> is the topic of Google cloud Pub/Sub
          |     <records-per-sec> is the rate of records per second to put onto the topic
          |
        """.stripMargin)
      System.exit(1)
    }

    val Seq(projectId, topic, recordsPerSecond) = args.toSeq

    val APP_NAME = this.getClass.getSimpleName

    val client = new Builder(
      GoogleNetHttpTransport.newTrustedTransport(),
      JacksonFactory.getDefaultInstance(),
      new RetryHttpInitializer(
        SparkGCPCredentials.builder.build().provider,
        APP_NAME
      ))
        .setApplicationName(APP_NAME)
        .build()

    val randomWords = List("google", "cloud", "pubsub", "say", "hello")
    val publishRequest = new PublishRequest()
    for (i <- 1 to 10) {
      val messages = (1 to recordsPerSecond.toInt).map { recordNum =>
          val randomWordIndex = Random.nextInt(randomWords.size)
          new PubsubMessage().encodeData(randomWords(randomWordIndex).getBytes())
      }
      publishRequest.setMessages(messages.asJava)
      client.projects().topics()
          .publish(s"projects/$projectId/topics/$topic", publishRequest)
          .execute()
      println(s"Published data. topic: $topic; Mesaage: $publishRequest")

      Thread.sleep(1000)
    }

  }
}
// scalastyle:on
