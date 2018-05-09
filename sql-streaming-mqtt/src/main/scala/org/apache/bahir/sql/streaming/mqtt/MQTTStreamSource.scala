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

import java.nio.charset.Charset
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Optional}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.{MemoryPersistence, MqttDefaultFilePersistence}

import org.apache.spark.sql._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset => OffsetV2}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import org.apache.bahir.utils.Logging


object MQTTStreamConstants {

  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val SCHEMA_DEFAULT = StructType(StructField("value", StringType)
    :: StructField("timestamp", TimestampType) :: Nil)
}

/**
 * A Text based mqtt stream source, it interprets the payload of each incoming message by converting
 * the bytes to String using Charset.defaultCharset as charset. Each value is associated with a
 * timestamp of arrival of the message on the source. It can be used to operate a window on the
 * incoming stream.
 *
 * @param brokerUrl url MqttClient connects to.
 * @param persistence an instance of MqttClientPersistence. By default it is used for storing
 *                    incoming messages on disk. If memory is provided as option, then recovery on
 *                    restart is not supported.
 * @param topic topic MqttClient subscribes to.
 * @param clientId clientId, this client is assoicated with. Provide the same value to recover
 *                 a stopped client.
 * @param messageParser parsing logic for processing incoming messages from Mqtt Server.
 * @param mqttConnectOptions an instance of MqttConnectOptions for this Source.
 * @param qos the maximum quality of service to subscribe each topic at.Messages published at
 *            a lower quality of service will be received at the published QoS. Messages
 *            published at a higher quality of service will be received using the QoS specified
 *            on the subscribe.
 */
class MQTTTextStreamSource(options: DataSourceOptions, brokerUrl: String, persistence:
    MqttClientPersistence, topic: String, clientId: String,
    messageParser: Array[Byte] => (String, Timestamp),
    mqttConnectOptions: MqttConnectOptions, qos: Int)
  extends MicroBatchReader with Logging {

  private var startOffset: OffsetV2 = _
  private var endOffset: OffsetV2 = _


  private val store = new LocalMessageStore(persistence)

  private val messages = new TrieMap[Long, (String, Timestamp)]

  @GuardedBy("this")
  private var currentOffset: LongOffset = LongOffset(-1L)

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)


  private var client: MqttClient = _

  private def fetchLastProcessedOffset(): LongOffset = {
    Try(store.maxProcessedOffset) match {
      case Success(x) =>
        log.info(s"Recovering from last stored offset $x")
        LongOffset(x)
      case Failure(e) => LongOffset(-1L)
    }
  }

  initialize()
  private def initialize(): Unit = {

    client = new MqttClient(brokerUrl, clientId, persistence)
    val callback = new MqttCallbackExtended() {

      override def messageArrived(topic_ : String, message: MqttMessage): Unit = synchronized {
        val offset = currentOffset.offset + 1L
        messages.put(offset, messageParser(message.getPayload))
        currentOffset = LongOffset(offset)
        log.info(s"Message arrived, $topic_ $message")
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
    // It is not possible to initialize offset without `client.connect`
    lastOffsetCommitted = fetchLastProcessedOffset()
    client.subscribe(topic, qos)
  }

  override def setOffsetRange(
      start: Optional[OffsetV2], end: Optional[OffsetV2]): Unit = synchronized {
    startOffset = start.orElse(LongOffset(-1L))
    endOffset = end.orElse(currentOffset)
  }

  override def getStartOffset(): OffsetV2 = {
    Option(startOffset).getOrElse(throw new IllegalStateException("start offset not set"))
  }

  override def getEndOffset(): OffsetV2 = {
    Option(endOffset).getOrElse(throw new IllegalStateException("end offset not set"))
  }

  override def deserializeOffset(json: String): OffsetV2 = {
    LongOffset(json.toLong)
  }

  override def readSchema(): StructType = {
    MQTTStreamConstants.SCHEMA_DEFAULT
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList: IndexedSeq[(String, Timestamp)] = synchronized {
      val sliceStart = LongOffset.convert(startOffset).get.offset + 1
      val sliceEnd = LongOffset.convert(endOffset).get.offset + 1
      for ( i <- sliceStart until sliceEnd) yield messages(i)
    }

    val spark = SparkSession.getActiveSession.get
    val numPartitions = spark.sparkContext.defaultParallelism

    val slices = Array.fill(numPartitions)(new ListBuffer[(String, Timestamp)])
    rawList.zipWithIndex.foreach { case (r, idx) =>
      slices(idx % numPartitions).append(r)
    }

    (0 until numPartitions).map { i =>
      val slice = slices(i)
      new DataReaderFactory[Row] {
        override def createDataReader(): DataReader[Row] = new DataReader[Row] {
          private var currentIdx = -1

          override def next(): Boolean = {
            currentIdx += 1
            currentIdx < slice.size
          }

          override def get(): Row = {
            Row(slice(currentIdx)._1, slice(currentIdx)._2)
          }

          override def close(): Unit = {}
        }
      }
    }.toList.asJava
  }

  override def commit(end: OffsetV2): Unit = synchronized {
    val newOffset = LongOffset.convert(end).getOrElse(
      sys.error(s"MQTTStreamSource.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    (lastOffsetCommitted.offset until newOffset.offset).foreach { x =>
      messages.remove(x + 1)
    }
    lastOffsetCommitted = newOffset
  }

  /** Stop this source. */
  override def stop(): Unit = synchronized {
    client.disconnect()
    persistence.close()
    client.close()
  }

  override def toString: String = s"MQTTStreamSource[brokerUrl: $brokerUrl, topic: $topic" +
    s" clientId: $clientId]"
}

class MQTTStreamSourceProvider extends DataSourceV2
  with MicroBatchReadSupport with DataSourceRegister with Logging {

  override def createMicroBatchReader(schema: Optional[StructType],
      checkpointLocation: String, parameters: DataSourceOptions): MicroBatchReader = {
    if (schema.isPresent) {
      throw
        new IllegalArgumentException("The mqtt source does not support a user-specified schema.")
    }
    def e(s: String) = new IllegalArgumentException(s)

    val brokerUrl = parameters.get("brokerUrl").orElse(parameters.get("path").orElse(null))

    if (brokerUrl == null) {
      throw e("Please provide a broker url, with option(\"brokerUrl\", ...).")
    }

    val persistence: MqttClientPersistence = parameters.get("persistence").orElse("") match {
      case "memory" => new MemoryPersistence()
      case _ => val localStorage: String = parameters.get("localStorage").orElse("")
        localStorage match {
          case x => new MqttDefaultFilePersistence(x)
          case _ => new MqttDefaultFilePersistence()
        }
    }

    val messageParserWithTimeStamp = (x: Array[Byte]) =>
      (new String(x, Charset.defaultCharset()), Timestamp.valueOf(
        MQTTStreamConstants.DATE_FORMAT.format(Calendar.getInstance().getTime)))

    // if default is subscribe everything, it leads to getting lot unwanted system messages.
    val topic: String = parameters.get("topic").orElse(null)
    if (topic == null) {
      throw e("Please specify a topic, by .options(\"topic\",...)")
    }

    val clientId: String = parameters.get("clientId").orElse {
      log.warn("If `clientId` is not set, a random value is picked up." +
        " Recovering from failure is not supported in such a case.")
      MqttClient.generateClientId()}

    val username: String = parameters.get("username").orElse(null)
    val password: String = parameters.get("password").orElse(null)

    val connectionTimeout: Int = parameters.get("connectionTimeout").orElse(
      MqttConnectOptions.CONNECTION_TIMEOUT_DEFAULT.toString).toInt
    val keepAlive: Int = parameters.get("keepAlive").orElse(MqttConnectOptions
      .KEEP_ALIVE_INTERVAL_DEFAULT.toString).toInt
    val mqttVersion: Int = parameters.get("mqttVersion").orElse(MqttConnectOptions
      .MQTT_VERSION_DEFAULT.toString).toInt
    val cleanSession: Boolean = parameters.get("cleanSession").orElse("false").toBoolean
    val qos: Int = parameters.get("QoS").orElse("1").toInt

    val mqttConnectOptions: MqttConnectOptions = new MqttConnectOptions()
    mqttConnectOptions.setAutomaticReconnect(true)
    mqttConnectOptions.setCleanSession(cleanSession)
    mqttConnectOptions.setConnectionTimeout(connectionTimeout)
    mqttConnectOptions.setKeepAliveInterval(keepAlive)
    mqttConnectOptions.setMqttVersion(mqttVersion)
    (username, password) match {
      case (u: String, p: String) if u != null && p != null =>
        mqttConnectOptions.setUserName(u)
        mqttConnectOptions.setPassword(p.toCharArray)
      case _ =>
    }

    new  MQTTTextStreamSource(parameters, brokerUrl, persistence, topic, clientId,
      messageParserWithTimeStamp, mqttConnectOptions, qos)
  }
  override def shortName(): String = "mqtt"
}
