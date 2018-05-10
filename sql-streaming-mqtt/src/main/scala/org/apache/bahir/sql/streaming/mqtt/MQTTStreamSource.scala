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
import java.util.{Calendar, HashSet => JHashSet, Optional}
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
import org.apache.spark.sql.types._

import org.apache.bahir.utils.Logging


object MQTTStreamConstants {

  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val SCHEMA_DEFAULT = StructType(StructField("id", IntegerType) :: StructField("topic",
    StringType):: StructField("payload", BinaryType) :: StructField("timestamp", TimestampType) ::
    Nil)
}

class MQTTMessage(m: MqttMessage, val topic: String) extends Serializable {

  // TODO: make it configurable.
  val timestamp: Timestamp = Timestamp.valueOf(
    MQTTStreamConstants.DATE_FORMAT.format(Calendar.getInstance().getTime))
  val duplicate = m.isDuplicate
  val retained = m.isRetained
  val qos = m.getQos
  val id: Int = m.getId

  val payload: Array[Byte] = m.getPayload

  override def toString(): String = {
    s"""MQTTMessage.
       |Topic: ${this.topic}
       |MessageID: ${this.id}
       |QoS: ${this.qos}
       |Payload: ${this.payload}
       |Payload as string: ${new String(this.payload, Charset.defaultCharset())}
       |isRetained: ${this.retained}
       |isDuplicate: ${this.duplicate}
       |TimeStamp: ${this.timestamp}
     """.stripMargin
  }
}
/**
 * A mqtt stream source.
 *
 * @param brokerUrl url MqttClient connects to.
 * @param persistence an instance of MqttClientPersistence. By default it is used for storing
 *                    incoming messages on disk. If memory is provided as option, then recovery on
 *                    restart is not supported.
 * @param topic topic MqttClient subscribes to.
 * @param clientId clientId, this client is assoicated with. Provide the same value to recover
 *                 a stopped client.
 * @param mqttConnectOptions an instance of MqttConnectOptions for this Source.
 * @param qos the maximum quality of service to subscribe each topic at.Messages published at
 *            a lower quality of service will be received at the published QoS. Messages
 *            published at a higher quality of service will be received using the QoS specified
 *            on the subscribe.
 */
class MQTTStreamSource(options: DataSourceOptions, brokerUrl: String, persistence:
    MqttClientPersistence, topic: String, clientId: String,
    mqttConnectOptions: MqttConnectOptions, qos: Int)
  extends MicroBatchReader with Logging {

  private var startOffset: OffsetV2 = _
  private var endOffset: OffsetV2 = _

  /* Older than last N messages, will not be checked for redelivery. */
  val backLog = options.getInt("autopruning.backlog", 500)

  private val store = new LocalMessageStore(persistence)

  private val messages = new TrieMap[Long, MQTTMessage]

  @GuardedBy("this")
  private val processedMessageIds = new JHashSet[Int](backLog)

  private var maxIdProcessed = 0

  @GuardedBy("this")
  private var currentOffset: LongOffset = LongOffset(-1L)

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private var client: MqttClient = _

  private def fetchLastProcessedOffset(): LongOffset = {
    Try(store.maxProcessedOffset) match {
      case Success(x) => // Data processed so far, is not replayed again.
        log.info(s"Trying to resume from last processed offset $x")
        LongOffset(x)
      case Failure(e) => LongOffset(-1L)
    }
  }

  private[mqtt] def getCurrentOffset = currentOffset

  initialize()
  private def initialize(): Unit = {

    client = new MqttClient(brokerUrl, clientId, persistence)
    val callback = new MqttCallbackExtended() {

      override def messageArrived(topic_ : String, message: MqttMessage): Unit = synchronized {
        val mqttMessage = new MQTTMessage(message, topic_)
        if(!processedMessageIds.contains(mqttMessage.id)) {
          val offset = currentOffset.offset + 1L
          messages.put(offset, mqttMessage)
          currentOffset = LongOffset(offset)
          log.trace(s"Message arrived, $topic_ $mqttMessage")
        } else {
          log.debug(s"Ignored redelivery of $mqttMessage")
        }
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
    startOffset = lastOffsetCommitted
    endOffset = lastOffsetCommitted
    currentOffset = lastOffsetCommitted
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
    val set = new JHashSet[Int]()
    val rawList: IndexedSeq[Option[MQTTMessage]] = synchronized {
      val sliceStart = LongOffset.convert(startOffset).get.offset + 1
      val sliceEnd = LongOffset.convert(endOffset).get.offset + 1
      for ( i <- sliceStart until sliceEnd) yield {
        val m = messages(i)
        // Only process the messages not already processed.
        if (!processedMessageIds.contains(m.id) && !set.contains(m.id)) {
          set.add(m.id)
          if (maxIdProcessed < m.id) {
            maxIdProcessed = m.id
          }
          Some(m)
        } else None
      }
    }
    processedMessageIds.addAll(set)
    val spark = SparkSession.getActiveSession.get
    val numPartitions = spark.sparkContext.defaultParallelism

    val slices = Array.fill(numPartitions)(new ListBuffer[MQTTMessage])
    rawList.flatten.zipWithIndex.foreach { case (r, idx) =>
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
            Row(slice(currentIdx).id, slice(currentIdx).topic,
              slice(currentIdx).payload, slice(currentIdx).timestamp)
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
    if (processedMessageIds.size() > 2 * backLog) {
      // Prune extra messages.
      val toBePruned = processedMessageIds.asScala.filter(_ < (maxIdProcessed - backLog))
      toBePruned.foreach(processedMessageIds.remove)
      log.debug(s"Pruned processedMessageIds and removed ${toBePruned.size} entries.")
    }
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
    def e(s: String) = new IllegalArgumentException(s)
    if (schema.isPresent) {
      throw e("The mqtt source does not support a user-specified schema.")
    }

    val brokerUrl = parameters.get("brokerUrl").orElse(parameters.get("path").orElse(null))

    if (brokerUrl == null) {
      throw e("Please provide a broker url, with option(\"brokerUrl\", ...).")
    }

    val persistence: MqttClientPersistence = parameters.get("persistence").orElse("") match {
      case "memory" => new MemoryPersistence()
      case _ => val localStorage: String = parameters.get("localStorage").orElse("")
        localStorage match {
          case "" => new MqttDefaultFilePersistence()
          case x => new MqttDefaultFilePersistence(x)
        }
    }

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

    new  MQTTStreamSource(parameters, brokerUrl, persistence, topic, clientId,
      mqttConnectOptions, qos)
  }
  override def shortName(): String = "mqtt"
}
