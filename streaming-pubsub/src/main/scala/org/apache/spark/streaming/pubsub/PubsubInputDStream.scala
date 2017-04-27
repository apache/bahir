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

package org.apache.spark.streaming.pubsub

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.pubsub.Pubsub.Builder
import com.google.api.services.pubsub.model.{AcknowledgeRequest, PubsubMessage, PullRequest}
import com.google.cloud.hadoop.util.RetryHttpInitializer

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.Utils

private[streaming]
class PubsubInputDStream(
    _ssc: StreamingContext,
    val project: String,
    val subscription: String,
    val credential: Credential,
    val _storageLevel: StorageLevel
) extends ReceiverInputDStream[SparkPubsubMessage](_ssc) {

  override def getReceiver(): Receiver[SparkPubsubMessage] = {
    new PubsubReceiver(project, subscription, credential, _storageLevel)
  }
}

class SparkPubsubMessage(val message: PubsubMessage) extends Externalizable {

  def getData(): Array[Byte] = message.decodeData()

  def getAttributes(): java.util.Map[String, String] = message.getAttributes()

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    val data = message.decodeData()
    out.writeInt(data.size)
    out.write(data)

    val numAttributes = message.getAttributes.size()
    out.writeInt(numAttributes)
    for ((k, v) <- message.getAttributes.asScala) {
      val keyBuff = Utils.serialize(k)
      out.writeInt(keyBuff.length)
      out.write(keyBuff)
      val valBuff = Utils.serialize(v)
      out.writeInt(valBuff.length)
      out.write(valBuff)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val bodyLength = in.readInt()
    val data = new Array[Byte](bodyLength)
    in.readFully(data)

    val numAttributes = in.readInt()
    val attributes = new java.util.HashMap[String, String]

    for (i <- 0 until numAttributes) {
      val keyLength = in.readInt()
      val keyBuff = new Array[Byte](keyLength)
      in.readFully(keyBuff)
      val key: String = Utils.deserialize(keyBuff)

      val valLength = in.readInt()
      val valBuff = new Array[Byte](valLength)
      in.readFully(valBuff)
      val value: String = Utils.deserialize(valBuff)

      attributes.put(key, value)
    }

    message.encodeData(data)
    message.setAttributes(attributes)
  }
}

private [pubsub]
object Transport {
  val transport = GoogleNetHttpTransport.newTrustedTransport();
  val jacksonFactory = JacksonFactory.getDefaultInstance;
}


private[pubsub]
class PubsubReceiver(
    project: String,
    subscription: String,
    credential: Credential,
    storageLevel: StorageLevel)
    extends Receiver[SparkPubsubMessage](storageLevel) {

  val APP_NAME = "sparkstreaming-pubsub-receiver"

  val BACKOFF = 100 // 100ms

  val MAX_BACKOFF = 10 * 1000 // 10s

  val RESOURCE_EXHAUSTED = 429

  val CANCELLED = 499

  val INTERNAL = 500

  val UNAVAILABLE = 503

  val DEADLINE_EXCEEDED = 504

  val MAX_MESSAGE = 1000

  override def onStart(): Unit = {

    new Thread() {
      override def run() {
        receive()
      }
    }.start()

  }

  def receive(): Unit = {
    val client = new Builder(
      Transport.transport,
      Transport.jacksonFactory,
      new RetryHttpInitializer(credential, APP_NAME))
        .setApplicationName(APP_NAME)
        .build()
    val pullRequest = new PullRequest().setMaxMessages(MAX_MESSAGE).setReturnImmediately(false)
    val subscriptionFullName = s"projects/$project/subscriptions/$subscription"
    var backoff = BACKOFF
    while (!isStopped()) {
      try {
        val pullResponse =
          client.projects().subscriptions().pull(subscriptionFullName, pullRequest).execute()
        val receivedMessages = pullResponse.getReceivedMessages.asScala.toList
        store(receivedMessages
            .map(x => new SparkPubsubMessage(x.getMessage))
            .iterator)

        val ackRequest = new AcknowledgeRequest()
        ackRequest.setAckIds(receivedMessages.map(x => x.getAckId).asJava)
        client.projects().subscriptions().acknowledge(subscriptionFullName, ackRequest)
      } catch {
        case e: GoogleJsonResponseException =>
          e.getDetails.getCode match {
            case RESOURCE_EXHAUSTED | CANCELLED | INTERNAL | UNAVAILABLE | DEADLINE_EXCEEDED =>
              Thread.sleep(backoff)
              backoff = Math.min(backoff * 2, MAX_BACKOFF)
          }
        case NonFatal(e) => reportError("Failed to pull messages", e)
      }
    }
  }

  override def onStop(): Unit = {}
}
