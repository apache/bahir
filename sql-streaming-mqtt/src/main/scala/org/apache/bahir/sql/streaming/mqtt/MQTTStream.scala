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

package org.apache.bahir.sql.streaming.mqtt

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ArrayBuffer

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.{MemoryPersistence, MqttDefaultFilePersistence}

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}


object MQTTStream {

  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val SCHEMA_DEFAULT = StructType(StructField("value", StringType)
    :: StructField("timestamp", TimestampType) :: Nil)
}

class MQTTTextStream(brokerUrl: String, persistence: MqttClientPersistence,
    topic: String, messageParser: Array[Byte] => (String, Timestamp),
    sqlContext: SQLContext) extends Source with Logging {

  override def schema: StructType = MQTTStream.SCHEMA_DEFAULT

  @GuardedBy("this")
  private var messages = new ArrayBuffer[(String, Timestamp)]
  initialize()
  private def initialize(): Unit = {

    val client = new MqttClient(brokerUrl, MqttClient.generateClientId(), persistence)
    val mqttConnectOptions: MqttConnectOptions = new MqttConnectOptions()
    mqttConnectOptions.setAutomaticReconnect(true)
    val callback = new MqttCallbackExtended() {

      override def messageArrived(topic_ : String, message: MqttMessage): Unit = {
        messages += messageParser(message.getPayload)
        log.trace(s"Message arrived, $topic_ $message")
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
      }

      override def connectionLost(cause: Throwable): Unit = {
        log.warn("Connection to mqtt server lost.", cause)
      }

      override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
        log.info(s"connect complete $serverURI. Is it a reconnect?: $reconnect")
      }
    }
    client.setCallback(callback)
    client.connect(mqttConnectOptions)
    client.subscribe(topic)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = {
  }

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    if (messages.isEmpty) {
      None
    } else {
      Some(LongOffset(messages.size))
    }
  }

  /**
   * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None` then
   * the batch should begin with the first available record. This method must always return the
   * same data for a particular `start` and `end` pair.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    val startIndex = start.getOrElse(LongOffset(0L)).asInstanceOf[LongOffset].offset.toInt
    val endIndex = end.asInstanceOf[LongOffset].offset.toInt
    val data: ArrayBuffer[(String, Timestamp)] = messages.slice(startIndex, endIndex)
    log.trace(s"Get Batch invoked, ${data.mkString}")
    import sqlContext.implicits._
    data.toDF("value", "timestamp")
  }
}

class MQTTStreamProvider extends StreamSourceProvider with DataSourceRegister with Logging {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
      providerName: String, parameters: Map[String, String]): (String, StructType) = {
    log.warn("The mqtt source should not be used for production applications!" +
      "It does not support recovery and stores state indefinitely.")
    ("mqttSource", MQTTStream.SCHEMA_DEFAULT)
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
      schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    def e(s: String) = new IllegalArgumentException()

    val brokerUrl: String = parameters.getOrElse("brokerUrl", parameters.getOrElse("path",
      throw e("Please provide a `brokerUrl` by specifying path or brokerUrl in options.")))

    val persistence: MqttClientPersistence = parameters.get("persistence") match {
      case Some("memory") => new MemoryPersistence()
      case _ => new MqttDefaultFilePersistence()
    }

    val messageParserWithTimeStamp = (x: Array[Byte]) => (new String(x), Timestamp.valueOf(
      MQTTStream.DATE_FORMAT.format(Calendar.getInstance().getTime)))

    val topic: String = parameters.getOrElse("topic", "#") // default is subscribe everything.

    new MQTTTextStream(brokerUrl, persistence, topic,
      messageParserWithTimeStamp, sqlContext)
  }

  override def shortName(): String = "mqtt"
}
