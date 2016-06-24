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
package org.apache.spark.examples.streaming.zeromq

import scala.language.implicitConversions

import akka.actor.ActorSystem
import akka.actor.actorRef2Scala
import akka.util.ByteString
import akka.zeromq._
import akka.zeromq.Subscribe
import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.zeromq._

/**
 * A simple publisher for demonstration purposes, repeatedly publishes random Messages
 * every one second.
 */
object SimpleZeroMQPublisher {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SimpleZeroMQPublisher <zeroMQUrl> <topic> ")
      System.exit(1)
    }

    val Seq(url, topic) = args.toSeq
    val acs: ActorSystem = ActorSystem()

    val pubSocket = ZeroMQExtension(acs).newSocket(SocketType.Pub, Bind(url))
    implicit def stringToByteString(x: String): ByteString = ByteString(x)
    val messages: List[ByteString] = List("words ", "may ", "count ")
    while (true) {
      Thread.sleep(1000)
      pubSocket ! ZMQMessage(ByteString(topic) :: messages)
    }
    acs.awaitTermination()
  }
}

// scalastyle:off
/**
 * A sample wordcount with ZeroMQStream stream
 *
 * To work with zeroMQ, some native libraries have to be installed.
 * Install zeroMQ (release 2.1) core libraries. [ZeroMQ Install guide]
 * (http://www.zeromq.org/intro:get-the-software)
 *
 * Usage: ZeroMQWordCount <zeroMQurl> <topic>
 *   <zeroMQurl> and <topic> describe where zeroMq publisher is running.
 *
 * To run this example locally, you may run publisher as
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.zeromq.SimpleZeroMQPublisher tcp://127.0.0.1:1234 foo`
 * and run the example as
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.zeromq.ZeroMQWordCount tcp://127.0.0.1:1234 foo`
 */
// scalastyle:on
object ZeroMQWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ZeroMQWordCount <zeroMQurl> <topic>")
      System.exit(1)
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val Seq(url, topic) = args.toSeq
    val sparkConf = new SparkConf().setAppName("ZeroMQWordCount")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    // Create the context and set the batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    def bytesToStringIterator(x: Seq[ByteString]): Iterator[String] = x.map(_.utf8String).iterator

    // For this stream, a zeroMQ publisher should be running.
    val lines = ZeroMQUtils.createStream(
      ssc,
      url,
      Subscribe(topic),
      bytesToStringIterator _)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
