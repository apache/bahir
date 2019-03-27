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

import java.io._
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.eclipse.paho.client.mqttv3.{MqttClientPersistence, MqttPersistable, MqttPersistenceException}
import org.eclipse.paho.client.mqttv3.internal.MqttPersistentData
import scala.util.Try

import org.apache.bahir.utils.Logging


/** A message store for MQTT stream source for SQL Streaming. */
trait MessageStore {

  /** Store a single id and corresponding serialized message */
  def store[T](id: Long, message: T): Boolean

  /** Retrieve message corresponding to a given id. */
  def retrieve[T](id: Long): T

  /** Highest offset we have stored */
  def maxProcessedOffset: Long

  /** Remove message corresponding to a given id. */
  def remove[T](id: Long): Unit

}

private[mqtt] class MqttPersistableData(bytes: Array[Byte]) extends MqttPersistable {
  override def getHeaderLength: Int = bytes.length
  override def getHeaderOffset: Int = 0
  override def getPayloadOffset: Int = 0
  override def getPayloadBytes: Array[Byte] = null
  override def getHeaderBytes: Array[Byte] = bytes
  override def getPayloadLength: Int = 0
}

trait Serializer {
  def deserialize[T](x: Array[Byte]): T
  def serialize[T](x: T): Array[Byte]
}

class JavaSerializer extends Serializer with Logging {

  override def deserialize[T](x: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(x)
    val in = new ObjectInputStream(bis)
    val obj = if (in != null) {
      val o = in.readObject()
      Try(in.close()).recover { case t: Throwable => log.warn("failed to close stream", t) }
      o
    } else {
      null
    }
    obj.asInstanceOf[T]
  }

  override def serialize[T](x: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(x)
    out.flush()
    if (bos != null) {
      val bytes: Array[Byte] = bos.toByteArray
      Try(bos.close()).recover { case t: Throwable => log.warn("failed to close stream", t) }
      bytes
    } else {
      null
    }
  }

}

object JavaSerializer {
  private lazy val instance = new JavaSerializer()
  def getInstance(): JavaSerializer = instance
}

/**
 * A message store to persist messages received. This is not intended to be thread safe.
 * It uses `MqttDefaultFilePersistence` for storing messages on disk locally on the client.
 */
private[mqtt] class LocalMessageStore(val persistentStore: MqttClientPersistence,
    val serializer: Serializer) extends MessageStore with Logging {

  def this(persistentStore: MqttClientPersistence) =
    this(persistentStore, JavaSerializer.getInstance())

  private def get(id: Long) = {
    persistentStore.get(id.toString).getHeaderBytes
  }

  import scala.collection.JavaConverters._

  override def maxProcessedOffset: Long = {
    val keys: util.Enumeration[_] = persistentStore.keys()
    keys.asScala.map(x => x.toString.toInt).max
  }

  /** Store a single id and corresponding serialized message */
  override def store[T](id: Long, message: T): Boolean = {
    val bytes: Array[Byte] = serializer.serialize(message)
    try {
      persistentStore.put(id.toString, new MqttPersistableData(bytes))
      true
    } catch {
      case e: MqttPersistenceException => log.warn(s"Failed to store message Id: $id", e)
        false
    }
  }

  /** Retrieve message corresponding to a given id. */
  override def retrieve[T](id: Long): T = {
    serializer.deserialize(get(id))
  }

  override def remove[T](id: Long): Unit = {
    persistentStore.remove(id.toString)
  }

}

private[mqtt] class HdfsMqttClientPersistence(config: Configuration)
    extends MqttClientPersistence {

  var rootPath: Path = _
  var fileSystem: FileSystem = _

  override def open(clientId: String, serverURI: String): Unit = {
    try {
      rootPath = new Path("mqtt/" + clientId + "/" + serverURI.replaceAll("[^a-zA-Z0-9]", "_"))
      fileSystem = FileSystem.get(config)
      if (!fileSystem.exists(rootPath)) {
        fileSystem.mkdirs(rootPath)
      }
    }
    catch {
      case e: Exception => throw new MqttPersistenceException(e)
    }
  }

  override def close(): Unit = {
    try {
      fileSystem.close()
    }
    catch {
      case e: Exception => throw new MqttPersistenceException(e)
    }
  }

  override def put(key: String, persistable: MqttPersistable): Unit = {
    try {
      val path = getPath(key)
      val output = fileSystem.create(path)
      output.writeInt(persistable.getHeaderLength)
      if (persistable.getHeaderLength > 0) {
        output.write(persistable.getHeaderBytes)
      }
      output.writeInt(persistable.getPayloadLength)
      if (persistable.getPayloadLength > 0) {
        output.write(persistable.getPayloadBytes)
      }
      output.close()
    }
    catch {
      case e: Exception => throw new MqttPersistenceException(e)
    }
  }

  override def get(key: String): MqttPersistable = {
    try {
      val input = fileSystem.open(getPath(key))
      val headerLength = input.readInt()
      val headerBytes: Array[Byte] = new Array[Byte](headerLength)
      input.read(headerBytes)
      val payloadLength = input.readInt()
      val payloadBytes: Array[Byte] = new Array[Byte](payloadLength)
      input.read(payloadBytes)
      input.close()
      new MqttPersistentData(
        key, headerBytes, 0, headerBytes.length, payloadBytes, 0, payloadBytes.length
      )
    }
    catch {
      case e: Exception => throw new MqttPersistenceException(e)
    }
  }

  override def remove(key: String): Unit = {
    try {
      fileSystem.delete(getPath(key), false)
    }
    catch {
      case e: Exception => throw new MqttPersistenceException(e)
    }
  }

  override def keys(): util.Enumeration[String] = {
    try {
      val iterator = fileSystem.listFiles(rootPath, false)
      new util.Enumeration[String]() {
        override def hasMoreElements: Boolean = iterator.hasNext
        override def nextElement(): String = iterator.next().getPath.getName
      }
    }
    catch {
      case e: Exception => throw new MqttPersistenceException(e)
    }
  }

  override def clear(): Unit = {
    try {
      fileSystem.delete(rootPath, true)
    }
    catch {
      case e: Exception => throw new MqttPersistenceException(e)
    }
  }

  override def containsKey(key: String): Boolean = {
    try {
      fileSystem.isFile(getPath(key))
    }
    catch {
      case e: Exception => throw new MqttPersistenceException(e)
    }
  }

  private def getPath(key: String): Path = new Path(rootPath + "/" + key)

}
