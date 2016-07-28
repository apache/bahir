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

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer

import org.apache.bahir.utils.Logging
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.{MemoryPersistence, MqttDefaultFilePersistence}

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}


object MQTTStreamConstants {

  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val SCHEMA_DEFAULT = StructType(StructField("value", StringType)
    :: StructField("timestamp", TimestampType) :: Nil)
}

class MQTTTextStreamSource(brokerUrl: String, persistence: MqttClientPersistence,
    topic: String, messageParser: Array[Byte] => (String, Timestamp),
    sqlContext: SQLContext) extends Source with Logging {

  override def schema: StructType = MQTTStreamConstants.SCHEMA_DEFAULT

  private val store = new LocalMessageStore(persistence, sqlContext.sparkContext.getConf)

  private val messages = new TrieMap[Int, (String, Timestamp)]

  private var offset = 0

  initialize()
  private def initialize(): Unit = {

    val client = new MqttClient(brokerUrl, MqttClient.generateClientId(), persistence)
    val mqttConnectOptions: MqttConnectOptions = new MqttConnectOptions()
    mqttConnectOptions.setAutomaticReconnect(true)
    val callback = new MqttCallbackExtended() {

      override def messageArrived(topic_ : String, message: MqttMessage): Unit = synchronized {
        val temp = offset + 1
        messages.put(temp, messageParser(message.getPayload))
        offset = temp
        log.trace(s"Message arrived, $topic_ $message")
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
      }

      override def connectionLost(cause: Throwable): Unit = {
        log.warn("Connection to mqtt server lost.", cause)
      }

      override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
        log.info(s"Connect complete $serverURI. Is it a reconnect?: $reconnect")
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
    if (offset == 0) {
      None
    } else {
      Some(LongOffset(offset))
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
    val data: ArrayBuffer[(String, Timestamp)] = ArrayBuffer.empty
    val consumedMap: TrieMap[Int, (String, Timestamp)] = TrieMap.empty
    // Move consumed messages to persistent store.
    (startIndex + 1 to endIndex).foreach { id =>
      val element: (String, Timestamp) = messages.getOrElse(id, store.retrieve(id))
      data += element
      store.store(id, element)
      messages.remove(id, element)
    }
    log.trace(s"Get Batch invoked, ${data.mkString}")
    import sqlContext.implicits._
    data.toDF("value", "timestamp")
  }

}

class MQTTStreamSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
      providerName: String, parameters: Map[String, String]): (String, StructType) = {
    ("mqtt", MQTTStreamConstants.SCHEMA_DEFAULT)
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
      schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {

    def e(s: String) = new IllegalArgumentException(s)

    val brokerUrl: String = parameters.getOrElse("brokerUrl", parameters.getOrElse("path",
      throw e("Please provide a `brokerUrl` by specifying path or .options(\"brokerUrl\",...)")))


    val persistence: MqttClientPersistence = parameters.get("persistence") match {
      case Some("memory") => new MemoryPersistence()
      case _ => val localStorage: Option[String] = parameters.get("localStorage")
        localStorage match {
          case Some(x) => new MqttDefaultFilePersistence(x)
          case None => new MqttDefaultFilePersistence()
        }
    }

    val messageParserWithTimeStamp = (x: Array[Byte]) => (new String(x), Timestamp.valueOf(
      MQTTStreamConstants.DATE_FORMAT.format(Calendar.getInstance().getTime)))

    // if default is subscribe everything, it leads to getting lot unwanted system messages.
    val topic: String = parameters.getOrElse("topic",
      throw e("Please specify a topic, by .options(\"topic\",...)"))

    new MQTTTextStreamSource(brokerUrl, persistence, topic,
      messageParserWithTimeStamp, sqlContext)
  }

  override def shortName(): String = "mqtt"
}
