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

import java.nio.ByteBuffer
import java.util

import scala.reflect.ClassTag

import org.eclipse.paho.client.mqttv3.{MqttClientPersistence, MqttPersistable, MqttPersistenceException}

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerInstance}

import org.apache.bahir.utils.Logging


/** A message store for MQTT stream source for SQL Streaming. */
trait MessageStore {

  /** Store a single id and corresponding serialized message */
  def store[T: ClassTag](id: Int, message: T): Boolean

  /** Retrieve messages corresponding to certain offset range */
  def retrieve[T: ClassTag](start: Int, end: Int): Seq[T]

  /** Retrieve message corresponding to a given id. */
  def retrieve[T: ClassTag](id: Int): T

  /** Highest offset we have stored */
  def maxProcessedOffset: Int

}

private[mqtt] class MqttPersistableData(bytes: Array[Byte]) extends MqttPersistable {

  override def getHeaderLength: Int = bytes.length

  override def getHeaderOffset: Int = 0

  override def getPayloadOffset: Int = 0

  override def getPayloadBytes: Array[Byte] = null

  override def getHeaderBytes: Array[Byte] = bytes

  override def getPayloadLength: Int = 0
}

/**
 * A message store to persist messages received. This is not intended to be thread safe.
 * It uses `MqttDefaultFilePersistence` for storing messages on disk locally on the client.
 */
private[mqtt] class LocalMessageStore(val persistentStore: MqttClientPersistence,
    val serializer: Serializer) extends MessageStore with Logging {

  val classLoader = Thread.currentThread.getContextClassLoader

  def this(persistentStore: MqttClientPersistence, conf: SparkConf) =
    this(persistentStore, new JavaSerializer(conf))

  val serializerInstance: SerializerInstance = serializer.newInstance()

  private def get(id: Int) = {
    persistentStore.get(id.toString).getHeaderBytes
  }

  import scala.collection.JavaConverters._

  def maxProcessedOffset: Int = {
    val keys: util.Enumeration[_] = persistentStore.keys()
    keys.asScala.map(x => x.toString.toInt).max
  }

  /** Store a single id and corresponding serialized message */
  override def store[T: ClassTag](id: Int, message: T): Boolean = {
    val bytes: Array[Byte] = serializerInstance.serialize(message).array()
    try {
      persistentStore.put(id.toString, new MqttPersistableData(bytes))
      true
    } catch {
      case e: MqttPersistenceException => log.warn(s"Failed to store message Id: $id", e)
      false
    }
  }

  /** Retrieve messages corresponding to certain offset range */
  override def retrieve[T: ClassTag](start: Int, end: Int): Seq[T] = {
    (start until end).map(x => retrieve(x))
  }

  /** Retrieve message corresponding to a given id. */
  override def retrieve[T: ClassTag](id: Int): T = {
    serializerInstance.deserialize(ByteBuffer.wrap(get(id)), classLoader)
  }

}
