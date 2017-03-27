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

package org.apache.bahir.sql.streaming.akka

import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.rocksdb.RocksDB

import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerInstance}
import org.apache.spark.SparkConf

import org.apache.bahir.utils.Logging


trait MessageStore {

  def store[T: ClassTag](id: Int, message: T): Boolean

  def retrieve[T: ClassTag](start: Int, end: Int): Seq[Option[T]]

  def retrieve[T: ClassTag](id: Int): Option[T]

  def maxProcessedOffset: Int
}

private[akka] class LocalMessageStore(val persistentStore: RocksDB,
                                      val serializer: Serializer)
  extends MessageStore with Logging {

  val classLoader = Thread.currentThread().getContextClassLoader

  def this(persistentStore: RocksDB, conf: SparkConf) =
    this(persistentStore, new JavaSerializer(conf))

  val serializerInstance: SerializerInstance = serializer.newInstance()

  private def get(id: Int) = persistentStore.get(id.toString.getBytes)

  override def maxProcessedOffset: Int = persistentStore.getLatestSequenceNumber.toInt

  override def store[T: ClassTag](id: Int, message: T): Boolean = {
    val bytes: Array[Byte] = serializerInstance.serialize(message).array()
    try {
      persistentStore.put(id.toString.getBytes(), bytes)
      true
    } catch {
      case e: Exception => log.warn(s"Failed to store message Id: $id", e)
        false
    }
  }

  override def retrieve[T: ClassTag](start: Int, end: Int): Seq[Option[T]] = {
    (start until end).map(x => retrieve(x))
  }

  override def retrieve[T: ClassTag](id: Int): Option[T] = {
    val bytes = persistentStore.get(id.toString.getBytes)

    if (bytes != null) {
      Some(serializerInstance.deserialize(
        ByteBuffer.wrap(bytes), classLoader))
    } else {
      None
    }
  }
}
