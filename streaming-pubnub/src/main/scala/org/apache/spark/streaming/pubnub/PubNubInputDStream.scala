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

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import collection.JavaConverters._
import com.google.gson.JsonParser
import com.pubnub.api.PNConfiguration
import com.pubnub.api.PubNub
import com.pubnub.api.callbacks.SubscribeCallback
import com.pubnub.api.enums.PNReconnectionPolicy
import com.pubnub.api.models.consumer.PNStatus
import com.pubnub.api.models.consumer.pubsub.PNMessageResult
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.Utils

private[streaming]
class PubNubInputDStream(_ssc: StreamingContext,
    val configuration: PNConfiguration,
    val channels: Seq[String],
    val channelGroups: Seq[String],
    val timeToken: Option[Long],
    val _storageLevel: StorageLevel)
  extends ReceiverInputDStream[SparkPubNubMessage](_ssc) {
  override def getReceiver(): Receiver[SparkPubNubMessage] = {
    new PubNubReceiver(
      new SparkPubNubNConfiguration(configuration), channels, channelGroups,
      timeToken, _storageLevel
    )
  }
}

/**
 * Wrapper class for PNConfiguration with only consumer-related, serializable properties.
 * PubNub configuration model encapsulates various fields which are not serializable.
 */
private[pubnub]
class SparkPubNubNConfiguration(configuration: PNConfiguration) extends Serializable {
  var origin: String = configuration.getOrigin
  var subscribeTimeout: Integer = configuration.getSubscribeTimeout
  var secure: Boolean = configuration.isSecure
  var subscribeKey: String = configuration.getSubscribeKey
  var publishKey: String = configuration.getPublishKey
  var cipherKey: String = configuration.getCipherKey
  var authKey: String = configuration.getAuthKey
  var uuid: String = configuration.getUuid
  var connectTimeout: Integer = configuration.getConnectTimeout
  var filterExpression: String = configuration.getFilterExpression
  var reconnectionPolicy: PNReconnectionPolicy = configuration.getReconnectionPolicy
  var maximumReconnectionRetries: Integer = configuration.getMaximumReconnectionRetries
  var maximumConnections: Integer = configuration.getMaximumConnections

  def toConfiguration: PNConfiguration = {
    val config = new PNConfiguration()
    config.setOrigin(origin)
    config.setSubscribeTimeout(subscribeTimeout)
    config.setSecure(secure)
    config.setSubscribeKey(subscribeKey)
    config.setPublishKey(publishKey)
    config.setCipherKey(cipherKey)
    config.setAuthKey(authKey)
    config.setUuid(uuid)
    config.setConnectTimeout(connectTimeout)
    config.setFilterExpression(filterExpression)
    config.setReconnectionPolicy(reconnectionPolicy)
    config.setMaximumReconnectionRetries(maximumReconnectionRetries)
    config.setMaximumConnections(maximumConnections)
    config
  }
}

/**
 * Wrapper class for PNMessageResult with a custom serialization process.
 * PubNub message model uses GSON objects which are not serializable.
 */
class SparkPubNubMessage extends Externalizable {
  var message: PNMessageResult = _

  // PubNub does not support sending empty messages.
  def getPayload: String = message.getMessage.toString
  def getChannel: String = message.getChannel
  def getPublisher: String = message.getPublisher
  def getSubscription: String = message.getSubscription
  // Convert to Unix timestamp.
  def getTimestamp: Long = Math.ceil(message.getTimetoken / 10000).longValue()

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    def writeVariableLength(data: Any): Unit = {
      data match {
        case null => out.writeInt(-1)
        case d =>
          val buffer = Utils.serialize(d)
          out.writeInt(buffer.length)
          out.write(buffer)
      }
    }

    writeVariableLength(
      if (message.getMessage != null) message.getMessage.toString else null
    )
    writeVariableLength(message.getChannel)
    writeVariableLength(message.getPublisher)
    writeVariableLength(message.getSubscription)

    out.writeLong(message.getTimetoken)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    def readVariableLength(): Any = {
      in.readInt match {
        case -1 => null
        case length =>
          val buffer = new Array[Byte](length)
          in.readFully(buffer)
          Utils.deserialize(buffer)
      }
    }

    val parser = new JsonParser
    val builder = PNMessageResult.builder

    readVariableLength() match {
      case null =>
      case data => builder.message(parser.parse(data.asInstanceOf[String]))
    }

    builder.channel(readVariableLength().asInstanceOf[String])
    builder.publisher(readVariableLength().asInstanceOf[String])
    builder.subscription(readVariableLength().asInstanceOf[String])
    builder.timetoken(in.readLong())

    message = builder.build()
  }
}

private[pubnub]
class PubNubReceiver(configuration: SparkPubNubNConfiguration,
    channels: Seq[String],
    channelGroups: Seq[String],
    timeToken: Option[Long],
    storageLevel: StorageLevel)
  extends Receiver[SparkPubNubMessage](storageLevel) with Logging {

  var client: PubNub = _

  override def onStart(): Unit = {
    client = new PubNub(configuration.toConfiguration)
    client.addListener(
      new SubscribeCallback() {
        def status(pubNub: PubNub, status: PNStatus): Unit = {
          if (status.isError) {
            log.error(s"Encountered PubNub error: $status.")
          }
        }

        def message(pubNub: PubNub, message: PNMessageResult): Unit = {
          val record = new SparkPubNubMessage
          record.message = message
          store(record)
        }

        def presence(pubNub: PubNub, presence: PNPresenceEventResult): Unit = {
        }
      }
    )
    val builder = client.subscribe()
      .channels(channels.toList.asJava)
      .channelGroups(channelGroups.toList.asJava)
    if (timeToken.isDefined) {
      builder.withTimetoken(timeToken.get)
    }
    builder.execute()
  }

  override def onStop(): Unit = {
    client.unsubscribeAll()
    client.destroy()
  }
}
