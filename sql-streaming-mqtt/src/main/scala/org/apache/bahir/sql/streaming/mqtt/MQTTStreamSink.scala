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

import scala.collection.JavaConverters._

import org.eclipse.paho.client.mqttv3.MqttException

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.execution.streaming.sources.{PackedRowCommitMessage, PackedRowWriterFactory}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import org.apache.bahir.utils.Logging
import org.apache.bahir.utils.Retry


class MQTTStreamWriter (schema: StructType, parameters: DataSourceOptions)
    extends StreamWriter with Logging {
  private lazy val publishAttempts: Int =
    SparkEnv.get.conf.getInt("spark.mqtt.client.publish.attempts", -1)
  private lazy val publishBackoff: Long =
    SparkEnv.get.conf.getTimeAsMs("spark.mqtt.client.publish.backoff", "5s")

  assert(SparkSession.getActiveSession.isDefined)
  private val spark = SparkSession.getActiveSession.get

  private var topic: String = _
  private var qos: Int = -1

  initialize()
  private def initialize(): Unit = {
    val (_, _, topic_, _, _, qos_, _, _, _) = MQTTUtils.parseConfigParams(
      collection.immutable.HashMap() ++ parameters.asMap().asScala
    )
    topic = topic_
    qos = qos_
  }

  override def createWriterFactory(): DataWriterFactory[Row] = PackedRowWriterFactory

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    commit(messages)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val rows = messages.collect {
      case PackedRowCommitMessage(rs) => rs
    }.flatten

    // Skipping client identifier as single batch can be distributed to multiple
    // Spark worker process. MQTT server does not support two connections
    // declaring same client ID at given point in time.
    val params_ = Seq() ++ parameters.asMap().asScala.toSeq.filterNot(
      _._1.equalsIgnoreCase("clientId")
    )
    // IMPL Note: Had to declare new value reference due to serialization requirements.
    val topic_ = topic
    val qos_ = qos
    val publishAttempts_ = publishAttempts
    val publishBackoff_ = publishBackoff

    val data = spark.createDataFrame(rows.toList.asJava, schema)
    data.foreachPartition (
      iterator => iterator.foreach(
        row => {
          val client = CachedMQTTClient.getOrCreate(params_.toMap)
          val message = row.mkString.getBytes(Charset.defaultCharset())
          Retry(publishAttempts_, publishBackoff_, classOf[MqttException]) {
            // In case of errors, retry sending the message.
            client.publish(topic_, message, qos_, false)
          }
        }
      )
    )
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

case class MQTTRelation(override val sqlContext: SQLContext, data: DataFrame)
    extends BaseRelation {
  override def schema: StructType = data.schema
}

class MQTTStreamSinkProvider extends DataSourceV2 with StreamWriteSupport
    with DataSourceRegister with CreatableRelationProvider {
  override def createStreamWriter(queryId: String, schema: StructType,
      mode: OutputMode, options: DataSourceOptions): StreamWriter = {
    new MQTTStreamWriter(schema, options)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      parameters: Map[String, String], data: DataFrame): BaseRelation = {
    MQTTRelation(sqlContext, data)
  }

  override def shortName(): String = "mqtt"
}
