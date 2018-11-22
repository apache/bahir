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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import com.google.gson.JsonParser
import com.pubnub.api.models.consumer.pubsub.PNMessageResult

import org.apache.spark.SparkFunSuite

class MessageSerializationSuite extends SparkFunSuite {
  test("Full example") {
    checkMessageSerialization(
      "{\"message\":\"Hello, World!\"}", "channel1",
      "publisher1", "subscription1", System.currentTimeMillis * 10000
    )
  }

  test("Message from channel") {
    checkMessageSerialization("{\"message\":\"Hello, World!\"}", "c", "p", null, 13534398158620385L)
  }

  test("Message from subscription") {
    checkMessageSerialization("{\"message\":\"Hello, World!\"}", null, "p", "s", 13534397812467596L)
  }

  def checkMessageSerialization(payload: String, channel: String,
      publisher: String, subscription: String, timestamp: Long): Unit = {
    val builder = PNMessageResult.builder
      .message(if (payload != null) new JsonParser().parse(payload) else null)
      .channel(channel)
      .publisher(publisher)
      .subscription(subscription)
      .timetoken(timestamp)
    val pubNubMessage = builder.build()
    val sparkMessage = new SparkPubNubMessage
    sparkMessage.message = pubNubMessage

    // serializer
    val byteOutStream = new ByteArrayOutputStream
    val outputStream = new ObjectOutputStream(byteOutStream)
    outputStream.writeObject(sparkMessage)
    outputStream.flush()
    outputStream.close()
    byteOutStream.close()
    val serializedBytes = byteOutStream.toByteArray

    // deserialize
    val byteInStream = new ByteArrayInputStream(serializedBytes)
    val inputStream = new ObjectInputStream(byteInStream)
    val deserializedMessage = inputStream.readObject().asInstanceOf[SparkPubNubMessage]
    inputStream.close()
    byteInStream.close()

    assert(payload.equals(deserializedMessage.getPayload))
    if (channel != null) {
      assert(channel.equals(deserializedMessage.getChannel))
    } else {
      assert(deserializedMessage.getChannel == null)
    }
    if (subscription != null) {
      assert(subscription.equals(deserializedMessage.getSubscription))
    } else {
      assert(deserializedMessage.getSubscription == null)
    }
    assert(publisher.equals(deserializedMessage.getPublisher))
    val unixTimestamp = Math.ceil(timestamp / 10000).longValue()
    assert(unixTimestamp.equals(deserializedMessage.getTimestamp))
  }
}
