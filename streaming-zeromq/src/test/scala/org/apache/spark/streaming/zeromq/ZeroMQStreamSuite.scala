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

package org.apache.spark.streaming.zeromq

import scala.collection.mutable
import scala.language.postfixOps

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.scalatest.time
import org.scalatest.time.Span
import org.zeromq.Utils
import org.zeromq.ZContext
import org.zeromq.ZMQ
import org.zeromq.ZMsg

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

class ZeroMQStreamSuite extends SparkFunSuite with Eventually with BeforeAndAfter {
  private val publishUrl = "tcp://localhost:" + Utils.findOpenPort()
  private val topic1 = "topic1"
  private val topic2 = "topic2"
  private val messageConverter = (bytes: Array[Array[Byte]]) => {
    if (bytes(0) == null || bytes(0).length == 0) {
      // Just to test that topic name is correctly populated.
      // Should never happen, but it will cause test to fail.
      // Assertions are not serializable.
      Seq()
    } else {
      Seq(new String(bytes(1), ZMQ.CHARSET))
    }
  }

  private var ssc: StreamingContext = _
  private var zeroContext: ZContext = _
  private var zeroSocket: ZMQ.Socket = _

  before {
    ssc = new StreamingContext("local[2]", this.getClass.getSimpleName, Seconds(1))
  }

  after {
    if (zeroContext != null) {
      zeroContext.close()
      zeroContext = null
    }
    zeroSocket = null
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }

  test("Input stream API") {
    // Test the API, but do not exchange any messages.
    val test1: ReceiverInputDStream[String] = ZeroMQUtils.createStream(
      ssc, publishUrl, true, Seq(topic1.getBytes), messageConverter
    )
    val test2: ReceiverInputDStream[String] = ZeroMQUtils.createStream(
      ssc, publishUrl, true, Seq(topic1.getBytes), messageConverter,
      StorageLevel.MEMORY_AND_DISK_SER_2
    )
    val test3: ReceiverInputDStream[String] = ZeroMQUtils.createTextStream(
      ssc, publishUrl, true, Seq(topic1.getBytes)
    )
  }

  test("Publisher Bind(), Subscriber Connect()") {
    zeroContext = new ZContext
    zeroSocket = zeroContext.createSocket(ZMQ.PUB)
    zeroSocket.bind(publishUrl)

    val receiveStream = ZeroMQUtils.createStream(
      ssc, publishUrl, true, Seq(ZMQ.SUBSCRIPTION_ALL), messageConverter
    )

    @volatile var receivedMessages: mutable.Set[String] = mutable.Set()
    receiveStream.foreachRDD { rdd =>
      for (element <- rdd.collect()) {
        receivedMessages += element
      }
      receivedMessages
    }

    ssc.start()

    checkAllReceived(
      Map("Hello, World!" -> topic1, "Hello, ZeroMQ!" -> topic2),
      receivedMessages
    )
  }

  test("Publisher Connect(), Subscriber Bind()") {
    val receiveStream = ZeroMQUtils.createStream(
      ssc, publishUrl, false, Seq(ZMQ.SUBSCRIPTION_ALL), messageConverter
    )

    @volatile var receivedMessages: mutable.Set[String] = mutable.Set()
    receiveStream.foreachRDD { rdd =>
      for (element <- rdd.collect()) {
        receivedMessages += element
      }
      receivedMessages
    }

    ssc.start()

    zeroContext = new ZContext
    zeroSocket = zeroContext.createSocket(ZMQ.PUB)
    zeroSocket.connect(publishUrl)

    checkAllReceived(
      Map("Hello, World!" -> topic1, "Hello, ZeroMQ!" -> topic2),
      receivedMessages
    )
  }

  test("Filter by topic") {
    zeroContext = new ZContext
    zeroSocket = zeroContext.createSocket(ZMQ.PUB)
    zeroSocket.bind(publishUrl)

    val receiveStream = ZeroMQUtils.createStream(
      ssc, publishUrl, true, Seq(topic1.getBytes, topic2.getBytes), messageConverter
    )

    @volatile var receivedMessages: Set[String] = Set()
    receiveStream.foreachRDD { rdd =>
      for (element <- rdd.collect()) {
        receivedMessages += element
      }
      receivedMessages
    }

    ssc.start()

    eventually(timeout(Span(5, time.Seconds)), interval(Span(500, time.Millis))) {
      val payload1 = "Hello, World!"
      val payload2 = "Hello, 0MQ!"

      // First message should not be picked up.
      val msg1 = new ZMsg
      msg1.add("wrong-topic".getBytes)
      msg1.add("Bye, World!".getBytes)
      msg1.send(zeroSocket)

      // Second message should be received.
      val msg2 = new ZMsg
      msg2.add(topic1.getBytes)
      msg2.add(payload1.getBytes)
      msg2.send(zeroSocket)

      // Third message should be received.
      val msg3 = new ZMsg
      msg3.add(topic2.getBytes)
      msg3.add(payload2.getBytes)
      msg3.send(zeroSocket)

      assert(receivedMessages.size == 2)
      assert(Set(payload1, payload2).equals(receivedMessages))
    }
  }

  test("Multiple frame message") {
    zeroContext = new ZContext
    zeroSocket = zeroContext.createSocket(ZMQ.PUB)
    zeroSocket.bind(publishUrl)

    val receiveStream = ZeroMQUtils.createTextStream(
      ssc, publishUrl, true, Seq(topic1.getBytes, topic2.getBytes)
    )

    @volatile var receivedMessages: Set[String] = Set()
    receiveStream.foreachRDD { rdd =>
      for (element <- rdd.collect()) {
        receivedMessages += element
      }
      receivedMessages
    }

    ssc.start()

    eventually(timeout(Span(5, time.Seconds)), interval(Span(500, time.Millis))) {
      val part1 = "first line"
      val part2 = "second line"

      val msg = new ZMsg
      msg.add(topic1.getBytes)
      msg.add(part1.getBytes)
      msg.add(part2.getBytes)
      msg.send(zeroSocket)

      assert(receivedMessages.size == 2)
      assert(Set(part1, part2).equals(receivedMessages))
    }
  }

  test("Reconnection") {
    zeroContext = new ZContext
    zeroSocket = zeroContext.createSocket(ZMQ.PUB)
    zeroSocket.bind(publishUrl)

    val receiveStream = ZeroMQUtils.createStream(
      ssc, publishUrl, true, Seq(ZMQ.SUBSCRIPTION_ALL), messageConverter
    )

    @volatile var receivedMessages: mutable.Set[String] = mutable.Set()
    receiveStream.foreachRDD { rdd =>
      for (element <- rdd.collect()) {
        receivedMessages += element
      }
      receivedMessages
    }

    ssc.start()

    checkAllReceived(
      Map("Hello, World!" -> topic1, "Hello, ZeroMQ!" -> topic2),
      receivedMessages
    )

    // Terminate bounded socket (server).
    zeroContext.close()
    zeroSocket = null

    Thread.sleep(2000)

    // Create new socket without stopping Spark stream.
    zeroContext = new ZContext
    zeroSocket = zeroContext.createSocket(ZMQ.PUB)
    zeroSocket.bind(publishUrl)

    receivedMessages.clear()

    checkAllReceived(
      Map("Apache Spark" -> topic1, "Apache Kafka" -> topic2),
      receivedMessages
    )
  }

  def checkAllReceived(
      publishMessages: Map[String, String],
      receivedMessages: mutable.Set[String]): Unit = {
    eventually(timeout(Span(5, time.Seconds)), interval(Span(500, time.Millis))) {
      for ((k, v) <- publishMessages) {
        val msg = new ZMsg
        msg.add(v.getBytes)
        msg.add(k.getBytes)
        msg.send(zeroSocket)
      }
      assert(receivedMessages.size == publishMessages.size)
      assert(publishMessages.keySet.equals(receivedMessages))
    }
  }
}

