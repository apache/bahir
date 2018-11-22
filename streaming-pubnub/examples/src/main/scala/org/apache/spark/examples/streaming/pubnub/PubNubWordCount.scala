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

package org.apache.spark.examples.streaming.pubnub

import com.google.gson.JsonParser
import com.pubnub.api.PNConfiguration
import com.pubnub.api.enums.PNReconnectionPolicy

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.pubnub.{PubNubUtils, SparkPubNubMessage}

/**
 * Consumes messages from a PubNub channel and calculates word count.
 * For demo purpose, login to PubNub account and produce messages using Debug Console.
 * Expected message format: {"text": "Hello, World!"}
 *
 * Usage: PubNubWordCount <subscribeKey> <channel> <aggregationPeriodMS>
 *   <subscribeKey> subscribe key
 *   <channel> channel
 *   <aggregationPeriodMS> aggregation period in milliseconds
 *
 * Example:
 *  $ bin/run-example \
 *      org.apache.spark.examples.streaming.pubnub.PubNubWordCount \
 *         sub-c-2d245192-ee8d-11e8-b4c3-46cd67be4fbd my-channel 60000
 */
object PubNubWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      // scalastyle:off println
      System.err.println(
        """
          |Usage: PubNubWordCount <subscribeKey> <channel>
          |
          |     <subscribeKey>        subscribe key
          |     <channel>             channel
          |     <aggregationPeriodMS> aggregation period in milliseconds
          |
        """.stripMargin
      )
      // scalastyle:on
      System.exit(1)
    }

    val Seq(subscribeKey, channel, aggregationPeriod) = args.toSeq

    val sparkConf = new SparkConf().setAppName("PubNubWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Milliseconds(aggregationPeriod.toLong))

    val config = new PNConfiguration
    config.setSubscribeKey(subscribeKey)
    config.setSecure(true)
    config.setReconnectionPolicy(PNReconnectionPolicy.LINEAR)

    val pubNubStream: ReceiverInputDStream[SparkPubNubMessage] = PubNubUtils.createStream(
      ssc, config, Seq(channel), Seq(), None, StorageLevel.MEMORY_AND_DISK_SER_2)

    val wordCounts = pubNubStream
        .flatMap(
          message => new JsonParser().parse(message.getPayload)
            .getAsJsonObject.get("text").getAsString.split("\\s")
        )
        .map(word => (word, 1))
        .reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

