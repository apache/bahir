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

package org.apache.spark.sql.mqtt

import java.io.IOException
import java.util.Calendar
import java.util.concurrent.locks.{Lock, ReentrantLock}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path, PathFilter}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import org.apache.bahir.sql.streaming.mqtt.{LongOffset, MQTTStreamConstants}

/**
 * A Text based mqtt stream source, it interprets the payload of each incoming message by converting
 * the bytes to String using Charset.defaultCharset as charset. Each value is associated with a
 * timestamp of arrival of the message on the source. It can be used to operate a window on the
 * incoming stream.
 *
 * @param sqlContext         Spark provided, SqlContext.
 * @param metadataPath       meta data path
 * @param brokerUrl          url MqttClient connects to.
 * @param topic              topic MqttClient subscribes to.
 * @param clientId           clientId, this client is assoicated with.
 *                           Provide the same value to recover a stopped client.
 * @param mqttConnectOptions an instance of MqttConnectOptions for this Source.
 * @param qos                the maximum quality of service to subscribe each topic at.
 *                           Messages published at a lower quality of service will be received
 *                           at the published QoS. Messages published at a higher quality of
 *                           service will be received using the QoS specified on the subscribe.
 * @param maxBatchNumber     the max message number to process in one batch.
 * @param maxBatchSize       the max total size in one batch, measured in bytes number.
 */
class HdfsBasedMQTTStreamSource(
  sqlContext: SQLContext,
  metadataPath: String,
  brokerUrl: String,
  clientId: String,
  topic: String,
  mqttConnectOptions: MqttConnectOptions,
  qos: Int,
  maxBatchNumber: Long = Long.MaxValue,
  maxBatchSize: Long = Long.MaxValue,
  maxRetryNumber: Int = 3
) extends Source with Logging {

  import HDFSMQTTSourceProvider.SEP

  override def schema: StructType = MQTTStreamConstants.SCHEMA_DEFAULT

  // Last batch offset file index
  private var lastOffset: Long = -1L

  // Current data file index to write messages.
  private var currentMessageDataFileOffset: Long = 0L

  // FileSystem instance for storing received messages.
  private var fs: FileSystem = _
  private var messageStoreOutputStream: FSDataOutputStream = _

  // total message number received for current batch.
  private var messageNumberForCurrentBatch: Int = 0
  // total message size received for
  private var messageSizeForCurrentBatch: Int = 0

  private val minBatchesToRetain = sqlContext.sparkSession.sessionState.conf.minBatchesToRetain

  // the consecutive fail number, cannot exceed the `maxRetryNumber`
  private var consecutiveFailNum = 0

  private var client: MqttClient = _

  private val lock: Lock = new ReentrantLock()

  private val hadoopConfig: Configuration = if (HdfsBasedMQTTStreamSource.hadoopConfig != null) {
    logInfo("using setted hadoop configuration!")
    HdfsBasedMQTTStreamSource.hadoopConfig
  } else {
    logInfo("create a new configuration.")
    new Configuration()
  }

  private val rootCheckpointPath = {
    val path = new Path(metadataPath).getParent.getParent.toUri.toString
    logInfo(s"get rootCheckpointPath $path")
    path
  }

  private val receivedDataPath = s"$rootCheckpointPath/receivedMessages"

  // lazily init latest offset from offset WAL log
  private lazy val recoveredLatestOffset = {
    // the index of this source, parsing from metadata path
    val currentSourceIndex = {
      if (!metadataPath.isEmpty) {
        metadataPath.substring(metadataPath.lastIndexOf("/") + 1).toInt
      } else {
        -1
      }
    }
    if (currentSourceIndex >= 0) {
      val offsetLog = new OffsetSeqLog(sqlContext.sparkSession,
        new Path(rootCheckpointPath, "offsets").toUri.toString)
      // get the latest offset from WAL log
      offsetLog.getLatest() match {
        case Some((batchId, _)) =>
          logInfo(s"get latest batch $batchId")
          Some(batchId)
        case None =>
          logInfo("no offset avaliable in offset log")
          None
      }
    } else {
      logInfo("checkpoint path is not set")
      None
    }
  }

  initialize()

  // Change data file if reach flow control threshold for one batch.
  // Not thread safe.
  private def startWriteNewDataFile(): Unit = {
    if (messageStoreOutputStream != null) {
      logInfo(s"Need to write a new data file,"
        + s" close current data file index $currentMessageDataFileOffset")
      messageStoreOutputStream.flush()
      messageStoreOutputStream.hsync()
      messageStoreOutputStream.close()
      messageStoreOutputStream = null
    }
    currentMessageDataFileOffset += 1
    messageSizeForCurrentBatch = 0
    messageNumberForCurrentBatch = 0
    messageStoreOutputStream = null
  }

  // not thread safe
  private def addReceivedMessageInfo(messageNum: Int, messageSize: Int): Unit = {
    messageSizeForCurrentBatch += messageSize
    messageNumberForCurrentBatch += messageNum
  }

  // not thread safe
  private def hasNewMessageForCurrentBatch(): Boolean = {
    currentMessageDataFileOffset > lastOffset + 1 || messageNumberForCurrentBatch > 0
  }

  private def withLock[T](body: => T): T = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  private def initialize(): Unit = {

    // recover lastOffset from WAL log
    if (recoveredLatestOffset.nonEmpty) {
      lastOffset = recoveredLatestOffset.get
      logInfo(s"Recover lastOffset value ${lastOffset}")
    }

    fs = FileSystem.get(hadoopConfig)

    // recover message data file offset from hdfs
    val dataPath = new Path(receivedDataPath)
    if (fs.exists(dataPath)) {
      val fileManager = CheckpointFileManager.create(dataPath, hadoopConfig)
      val dataFileIndexs = fileManager.list(dataPath, new PathFilter {
        private def isBatchFile(path: Path) = {
          try {
            path.getName.toLong
            true
          } catch {
            case _: NumberFormatException => false
          }
        }

        override def accept(path: Path): Boolean = isBatchFile(path)
      }).map(_.getPath.getName.toLong)
      if (dataFileIndexs.nonEmpty) {
        currentMessageDataFileOffset = dataFileIndexs.max + 1
        assert(currentMessageDataFileOffset >= lastOffset + 1,
          s"Recovered invalid message data file offset $currentMessageDataFileOffset,"
            + s"do not match with lastOffset $lastOffset")
        logInfo(s"Recovered last message data file offset: ${currentMessageDataFileOffset - 1}, "
          + s"start from $currentMessageDataFileOffset")
      } else {
        logInfo("No old data file exist, start data file index from 0")
        currentMessageDataFileOffset = 0
      }
    } else {
      logInfo(s"Create data dir $receivedDataPath, start data file index from 0")
      fs.mkdirs(dataPath)
      currentMessageDataFileOffset = 0
    }

    client = new MqttClient(brokerUrl, clientId, new MemoryPersistence())

    val callback = new MqttCallbackExtended() {

      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        withLock[Unit] {
          val messageSize = message.getPayload.size
          // check if have reached the max number or max size for current batch.
          if (messageNumberForCurrentBatch + 1 > maxBatchNumber
            || messageSizeForCurrentBatch + messageSize > maxBatchSize) {
            startWriteNewDataFile()
          }
          // write message content to data file
          if (messageStoreOutputStream == null) {
            val path = new Path(s"${receivedDataPath}/${currentMessageDataFileOffset}")
            if (fs.createNewFile(path)) {
              logInfo(s"Create new message data file ${path.toUri.toString} success!")
            } else {
              throw new IOException(s"${path.toUri.toString} already exist,"
                + s"make sure do use unique checkpoint path for each app.")
            }
            messageStoreOutputStream = fs.append(path)
          }

          messageStoreOutputStream.writeBytes(s"${message.getId}${SEP}")
          messageStoreOutputStream.writeBytes(s"${topic}${SEP}")
          val timestamp = Calendar.getInstance().getTimeInMillis().toString
          messageStoreOutputStream.writeBytes(s"${timestamp}${SEP}")
          messageStoreOutputStream.write(message.getPayload())
          messageStoreOutputStream.writeBytes("\n")
          addReceivedMessageInfo(1, messageSize)
          consecutiveFailNum = 0
          logInfo(s"Message arrived, topic: $topic, message payload $message, "
            + s"messageId: ${message.getId}, message size: ${messageSize}")
        }
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
        // callback for publisher, no need here.
      }

      override def connectionLost(cause: Throwable): Unit = {
        // auto reconnection is enabled, so just add a log here.
        withLock[Unit] {
          consecutiveFailNum += 1
          logWarning(s"Connection to mqtt server lost, "
            + s"consecutive fail number $consecutiveFailNum", cause)
        }
      }

      override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
        logInfo(s"Connect complete $serverURI. Is it a reconnect?: $reconnect")
      }
    }
    client.setCallback(callback)
    client.connect(mqttConnectOptions)
    client.subscribe(topic, qos)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = {
    logInfo("Stop mqtt source.")
    client.disconnect()
    client.close()
    withLock[Unit] {
      if (messageStoreOutputStream != null) {
        messageStoreOutputStream.hflush()
        messageStoreOutputStream.hsync()
        messageStoreOutputStream.close()
        messageStoreOutputStream = null
      }
      fs.close()
    }
  }

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    withLock[Option[Offset]] {
      assert(consecutiveFailNum < maxRetryNumber,
        s"Write message data fail continuously for ${maxRetryNumber} times.")
      val result = if (!hasNewMessageForCurrentBatch()) {
        if (lastOffset == -1) {
          // first submit and no message has arrived.
          None
        } else {
          // no message has arrived for this batch.
          Some(LongOffset(lastOffset))
        }
      } else {
        // check if currently write the batch to be executed.
        if (currentMessageDataFileOffset == lastOffset + 1) {
          startWriteNewDataFile()
        }
        lastOffset += 1
        Some(LongOffset(lastOffset))
      }
      logInfo(s"getOffset result $result")
      result
    }
  }

  /**
   * Returns the data that is between the offsets (`start`, `end`).
   * The batch return the data in file {checkpointPath}/receivedMessages/{end}.
   * `Start` and `end` value have the relationship: `end value` = `start value` + 1,
   * if `start` is not None.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    withLock[Unit]{
      assert(consecutiveFailNum < maxRetryNumber,
        s"Write message data fail continuously for ${maxRetryNumber} times.")
    }
    logInfo(s"getBatch with start = $start, end = $end")
    val endIndex = getOffsetValue(end)
    if (start.nonEmpty) {
      val startIndex = getOffsetValue(start.get)
      assert(startIndex + 1 == endIndex,
        s"start offset: ${startIndex} and end offset: ${endIndex} do not match")
    }
    logTrace(s"Create a data frame using hdfs file $receivedDataPath/$endIndex")
    val rdd = sqlContext.sparkContext.textFile(s"$receivedDataPath/$endIndex")
      .map{case str =>
        // calculate message in
        val idIndex = str.indexOf(SEP)
        val messageId = str.substring(0, idIndex).toInt
        // get topic
        var subStr = str.substring(idIndex + SEP.length)
        val topicIndex = subStr.indexOf(SEP)
        val topic = UTF8String.fromString(subStr.substring(0, topicIndex))
        // get timestamp
        subStr = subStr.substring(topicIndex + SEP.length)
        val timestampIndex = subStr.indexOf(SEP)
        /*
        val timestamp = Timestamp.valueOf(
          MQTTStreamConstants.DATE_FORMAT.format(subStr.substring(0, timestampIndex).toLong))
          */
        val timestamp = subStr.substring(0, timestampIndex).toLong
        // get playload
        subStr = subStr.substring(timestampIndex + SEP.length)
        val payload = UTF8String.fromString(subStr).getBytes
        InternalRow(messageId, topic, payload, timestamp)
      }
    sqlContext.internalCreateDataFrame(rdd, MQTTStreamConstants.SCHEMA_DEFAULT, true)
  }

  /**
   * Remove the data file for the offset.
   *
   * @param end the end of offset that all data has been committed.
   */
  override def commit(end: Offset): Unit = {
    val offsetValue = getOffsetValue(end)
    if (offsetValue >= minBatchesToRetain) {
      val deleteDataFileOffset = offsetValue - minBatchesToRetain
      try {
        fs.delete(new Path(s"$receivedDataPath/$deleteDataFileOffset"), false)
        logInfo(s"Delete committed offset data file $deleteDataFileOffset success!")
      } catch {
        case e: Exception =>
          logWarning(s"Delete committed offset data file $deleteDataFileOffset failed. ", e)
      }
    }
  }

  private def getOffsetValue(offset: Offset): Long = {
    val offsetValue = offset match {
      case o: LongOffset => o.offset
      case so: SerializedOffset =>
        so.json.toLong
    }
    offsetValue
  }
}
object HdfsBasedMQTTStreamSource {

  var hadoopConfig: Configuration = _
}
