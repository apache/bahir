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

package org.apache.spark.examples.streaming.zeromq

import scala.language.implicitConversions
import scala.util.Random

import org.apache.log4j.{Level, Logger}
import org.zeromq.ZContext
import org.zeromq.ZMQ
import org.zeromq.ZMQException
import org.zeromq.ZMsg

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.zeromq.ZeroMQUtils

/**
 * Simple publisher for demonstration purposes,
 * repeatedly publishes random messages every one second.
 */
object SimpleZeroMQPublisher {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      // scalastyle:off println
      System.err.println("Usage: SimpleZeroMQPublisher <zeroMqUrl> <topic>")
      // scalastyle:on println
      System.exit(1)
    }

    val Seq(url, topic) = args.toSeq
    val context = new ZContext
    val socket = context.createSocket(ZMQ.PUB)
    socket.bind(url)

    val zmqThread = new Thread(new Runnable {
      def run() {
        val messages = List("words", "may", "count infinitely")
        val random = new Random
        while (!Thread.currentThread.isInterrupted) {
          try {
            Thread.sleep(random.nextInt(1000))
            val msg = new ZMsg
            msg.add(topic.getBytes)
            msg.add(messages(random.nextInt(messages.size)).getBytes)
            msg.send(socket)
          } catch {
            case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode =>
              Thread.currentThread.interrupt()
            case e: InterruptedException =>
            case e: Throwable => throw e
          }
        }
      }
    })

    sys.addShutdownHook( {
      context.destroy()
      zmqThread.interrupt()
      zmqThread.join()
    } )

    zmqThread.start()
  }
}

/**
 * Sample word count with ZeroMQ stream.
 *
 * Usage: ZeroMQWordCount <zeroMqUrl> <topic>
 *   <zeroMqUrl> describes where ZeroMQ publisher is running
 *   <topic> defines logical message type
 *
 * To run this example locally, you may start publisher as:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.zeromq.SimpleZeroMQPublisher tcp://127.0.0.1:1234 foo`
 * and run the example as:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.zeromq.ZeroMQWordCount tcp://127.0.0.1:1234 foo`
 */
object ZeroMQWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      // scalastyle:off println
      System.err.println("Usage: ZeroMQWordCount <zeroMqUrl> <topic>")
      // scalastyle:on println
      System.exit(1)
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath).
    Logger.getRootLogger.setLevel(Level.WARN)

    val Seq(url, topic) = args.toSeq
    val sparkConf = new SparkConf().setAppName("ZeroMQWordCount")

    // Check Spark configuration for master URL, set it to local if not present.
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    // Create the context and set the batch size.
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val lines = ZeroMQUtils.createTextStream(
      ssc, url, true, Seq(topic.getBytes)
    )
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

