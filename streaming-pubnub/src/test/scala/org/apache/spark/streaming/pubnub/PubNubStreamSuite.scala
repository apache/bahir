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

package org.apache.spark.streaming.pubnub

import java.util.{Map => JMap}
import java.util.UUID

import collection.JavaConverters._
import com.google.gson.JsonObject
import com.pubnub.api.{PNConfiguration, PubNub, PubNubException}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.scalatest.time
import org.scalatest.time.Span

import org.apache.spark.ConditionalSparkFunSuite
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

class PubNubStreamSuite extends ConditionalSparkFunSuite with Eventually with BeforeAndAfter {
  val subscribeKey = "demo"
  val publishKey = "demo"
  val channel = "test"

  var ssc: StreamingContext = _
  var configuration: PNConfiguration = _
  var client: PubNub = _

  def shouldRunTest(): Boolean = sys.env.get("ENABLE_PUBNUB_TESTS").contains("1")

  override def beforeAll(): Unit = {
    configuration = new PNConfiguration()
    configuration.setSubscribeKey(subscribeKey)
    configuration.setPublishKey(publishKey)
    client = new PubNub(configuration)
  }

  override def afterAll(): Unit = {
    client.destroy()
  }

  before {
    ssc = new StreamingContext("local[2]", this.getClass.getSimpleName, Seconds(1))
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }

  testIf("Stream receives messages", () => PubNubStreamSuite.this.shouldRunTest()) {
    val nbOfMsg = 5
    var publishedMessages: List[JsonObject] = List()
    @volatile var receivedMessages: Set[SparkPubNubMessage] = Set()

    val receiveStream = PubNubUtils.createStream(
      ssc, configuration, Seq(channel), Seq(), None, StorageLevel.MEMORY_AND_DISK_SER_2
    )
    receiveStream.foreachRDD { rdd =>
      if (rdd.collect().length > 0) {
        receivedMessages = receivedMessages ++ List(rdd.first)
        receivedMessages
      }
    }
    ssc.start()

    (1 to nbOfMsg).foreach(
      _ => {
        val message = new JsonObject()
        message.addProperty("text", UUID.randomUUID().toString)
        publishedMessages = message :: publishedMessages
      }
    )

    eventually(timeout(Span(15, time.Seconds)), interval(Span(1000, time.Millis))) {
      publishedMessages.foreach(
        m => if (!receivedMessages.map(m => m.getPayload).contains(m.toString)) publishMessage(m)
      )
      assert(
        publishedMessages.map(m => m.toString).toSet
          .subsetOf(receivedMessages.map(m => m.getPayload))
      )
      assert(channel.equals(receivedMessages.head.getChannel))
    }
  }

  testIf("Message filtering", () => PubNubStreamSuite.this.shouldRunTest()) {
    val config = new PNConfiguration()
    config.setSubscribeKey(subscribeKey)
    config.setPublishKey(publishKey)
    config.setFilterExpression("language == 'english'")

    @volatile var receivedMessages: Set[SparkPubNubMessage] = Set()

    val receiveStream = PubNubUtils.createStream(
      ssc, config, Seq(channel), Seq(), None, StorageLevel.MEMORY_AND_DISK_SER_2
    )
    receiveStream.foreachRDD { rdd =>
      if (rdd.collect().length > 0) {
        receivedMessages = receivedMessages ++ List(rdd.first)
        receivedMessages
      }
    }
    ssc.start()

    eventually(timeout(Span(15, time.Seconds)), interval(Span(1000, time.Millis))) {
      val polishMessage = new JsonObject()
      polishMessage.addProperty("text", "dzien dobry")
      publishMessage(polishMessage, Map("language" -> "polish").asJava)

      val englishMessage = new JsonObject()
      englishMessage.addProperty("text", "good morning")
      publishMessage(englishMessage, Map("language" -> "english").asJava)

      assert(receivedMessages.map(m => m.getPayload).size == 1)
      assert(receivedMessages.head.getPayload.equals(englishMessage.toString))
    }
  }

  testIf("Test time token", () => PubNubStreamSuite.this.shouldRunTest()) {
    val config = new PNConfiguration()
    config.setSubscribeKey(subscribeKey)
    config.setPublishKey(publishKey)

    @volatile var receivedMessages: Set[SparkPubNubMessage] = Set()

    val currentTimeToken = client.time().sync().getTimetoken

    // Try to register subscriber with time token after we send first message.
    val receiveStream = PubNubUtils.createStream(
      ssc, config, Seq(channel), Seq(), Some(currentTimeToken + 5000*10000),
      StorageLevel.MEMORY_AND_DISK_SER_2
    )
    receiveStream.foreachRDD { rdd =>
      if (rdd.collect().length > 0) {
        receivedMessages = receivedMessages ++ List(rdd.first)
        receivedMessages
      }
    }
    ssc.start()

    // Give time for subscription to successfully register.
    Thread.sleep(1000)

    // Make sure we publish the message. Hopefully it will not take more than 5 seconds.
    // Otherwise we may see the message and test will fails.
    val message = new JsonObject()
    message.addProperty("text", "past")
    var timeToken = -1L
    while (timeToken == -1L) {
      timeToken = publishMessage(message = message, store = true)
      Thread.sleep(500)
    }

    eventually(timeout(Span(15, time.Seconds)), interval(Span(1000, time.Millis))) {
      val message = new JsonObject()
      message.addProperty("text", "future")
      publishMessage(message)

      assert(receivedMessages.map(m => m.getPayload).size == 1)
      assert(receivedMessages.head.getPayload.equals(message.toString))
    }
  }

  def publishMessage(message: JsonObject,
      metadata: JMap[String, String] = Map.empty[String, String].asJava,
      store: Boolean = false) : Long = {
    try {
      client.publish().channel(channel).meta(metadata)
        .message(message).shouldStore(store).sync().getTimetoken
    } catch {
      case e: PubNubException =>
        if (!e.getErrormsg.contains("Account quota exceeded (2/1000000)")) {
          // Ignore quota limits on demo account. We will retry.
          throw new RuntimeException(e)
        }
        -1
    }
  }
}
