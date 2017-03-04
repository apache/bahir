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

package org.apache.spark.streaming.mqtt

import scala.reflect.ClassTag

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object MQTTUtils {
  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param ssc           StreamingContext object
   * @param brokerUrl     Url of remote MQTT publisher
   * @param topic         Topic name to subscribe to
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createStream(
      ssc: StreamingContext,
      brokerUrl: String,
      topic: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String] = {
    new MQTTInputDStream(ssc, brokerUrl, topic, storageLevel)
  }


  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param ssc                StreamingContext object
   * @param brokerUrl          Url of remote MQTT publisher
   * @param topic              Topic name to subscribe to
   * @param storageLevel       RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param clientId           ClientId to use for the mqtt connection
   * @param username           Username for authentication to the mqtt publisher
   * @param password           Password for authentication to the mqtt publisher
   * @param cleanSession       Sets the mqtt cleanSession parameter
   * @param qos                Quality of service to use for the topic subscription
   * @param connectionTimeout  Connection timeout for the mqtt connection
   * @param keepAliveInterval  Keepalive interal for the mqtt connection
   * @param mqttVersion        Version to use for the mqtt connection
   */
  def createStream(
      ssc: StreamingContext,
      brokerUrl: String,
      topic: String,
      storageLevel: StorageLevel,
      clientId: Option[String],
      username: Option[String],
      password: Option[String],
      cleanSession: Option[Boolean],
      qos: Option[Int],
      connectionTimeout: Option[Int],
      keepAliveInterval: Option[Int],
      mqttVersion: Option[Int]
    ): ReceiverInputDStream[String] = {
    new MQTTInputDStream(ssc, brokerUrl, topic, storageLevel, clientId, username, password,
          cleanSession, qos, connectionTimeout, keepAliveInterval, mqttVersion)
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc      JavaStreamingContext object
   * @param brokerUrl Url of remote MQTT publisher
   * @param topic     Topic name to subscribe to
   */
  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topic: String
    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topic)
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param jssc          JavaStreamingContext object
   * @param brokerUrl     Url of remote MQTT publisher
   * @param topic         Topic name to subscribe to
   * @param storageLevel  RDD storage level.
   */
  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topic: String,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topic, storageLevel)
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param jssc               JavaStreamingContext object
   * @param brokerUrl          Url of remote MQTT publisher
   * @param topic              Topic name to subscribe to
   * @param storageLevel       RDD storage level.
   * @param clientId           ClientId to use for the mqtt connection
   * @param username           Username for authentication to the mqtt publisher
   * @param password           Password for authentication to the mqtt publisher
   * @param cleanSession       Sets the mqtt cleanSession parameter
   * @param qos                Quality of service to use for the topic subscription
   * @param connectionTimeout  Connection timeout for the mqtt connection
   * @param keepAliveInterval  Keepalive interal for the mqtt connection
   * @param mqttVersion        Version to use for the mqtt connection
   */
  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topic: String,
      storageLevel: StorageLevel,
      clientId: String,
      username: String,
      password: String,
      cleanSession: Boolean,
      qos: Int,
      connectionTimeout: Int,
      keepAliveInterval: Int,
      mqttVersion: Int
    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topic, storageLevel, Option(clientId), Option(username),
        Option(password), Option(cleanSession), Option(qos), Option(connectionTimeout),
        Option(keepAliveInterval), Option(mqttVersion))
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param jssc               JavaStreamingContext object
   * @param brokerUrl          Url of remote MQTT publisher
   * @param topic              Topic name to subscribe to
   * @param clientId           ClientId to use for the mqtt connection
   * @param username           Username for authentication to the mqtt publisher
   * @param password           Password for authentication to the mqtt publisher
   * @param cleanSession       Sets the mqtt cleanSession parameter
   * @param qos                Quality of service to use for the topic subscription
   * @param connectionTimeout  Connection timeout for the mqtt connection
   * @param keepAliveInterval  Keepalive interal for the mqtt connection
   * @param mqttVersion        Version to use for the mqtt connection
   */
  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topic: String,
      clientId: String,
      username: String,
      password: String,
      cleanSession: Boolean,
      qos: Int,
      connectionTimeout: Int,
      keepAliveInterval: Int,
      mqttVersion: Int
    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topic, StorageLevel.MEMORY_AND_DISK_SER_2, Option(clientId),
      Option(username), Option(password), Option(cleanSession), Option(qos),
      Option(connectionTimeout), Option(keepAliveInterval), Option(mqttVersion))
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param jssc               JavaStreamingContext object
   * @param brokerUrl          Url of remote MQTT publisher
   * @param topic              Topic name to subscribe to
   * @param clientId           ClientId to use for the mqtt connection
   * @param username           Username for authentication to the mqtt publisher
   * @param password           Password for authentication to the mqtt publisher
   * @param cleanSession       Sets the mqtt cleanSession parameter
   */
  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topic: String,
      clientId: String,
      username: String,
      password: String,
      cleanSession: Boolean
    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topic, StorageLevel.MEMORY_AND_DISK_SER_2, Option(clientId),
      Option(username), Option(password), Option(cleanSession), None, None, None, None)
  }
  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param ssc           StreamingContext object
   * @param brokerUrl     Url of remote MQTT publisher
   * @param topics        Array of topic names to subscribe to
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createPairedStream(
      ssc: StreamingContext,
      brokerUrl: String,
      topics: Array[String],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[(String, String)] = {
    new MQTTPairedInputDStream(ssc, brokerUrl, topics, storageLevel)
  }


  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param ssc                StreamingContext object
   * @param brokerUrl          Url of remote MQTT publisher
   * @param topics             Array of topic names to subscribe to
   * @param storageLevel       RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param clientId           ClientId to use for the mqtt connection
   * @param username           Username for authentication to the mqtt publisher
   * @param password           Password for authentication to the mqtt publisher
   * @param cleanSession       Sets the mqtt cleanSession parameter
   * @param qos                Quality of service to use for the topic subscription
   * @param connectionTimeout  Connection timeout for the mqtt connection
   * @param keepAliveInterval  Keepalive interal for the mqtt connection
   * @param mqttVersion        Version to use for the mqtt connection
   */
  def createPairedStream(
      ssc: StreamingContext,
      brokerUrl: String,
      topics: Array[String],
      storageLevel: StorageLevel,
      clientId: Option[String],
      username: Option[String],
      password: Option[String],
      cleanSession: Option[Boolean],
      qos: Option[Int],
      connectionTimeout: Option[Int],
      keepAliveInterval: Option[Int],
      mqttVersion: Option[Int]
    ): ReceiverInputDStream[(String, String)] = {
    new MQTTPairedInputDStream(ssc, brokerUrl, topics, storageLevel, clientId, username, password,
          cleanSession, qos, connectionTimeout, keepAliveInterval, mqttVersion)
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc      JavaStreamingContext object
   * @param brokerUrl Url of remote MQTT publisher
   * @param topics    Array of topic names to subscribe to
   */
  def createPairedStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String]
    ): JavaReceiverInputDStream[(String, String)] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createPairedStream(jssc.ssc, brokerUrl, topics)
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param jssc          JavaStreamingContext object
   * @param brokerUrl     Url of remote MQTT publisher
   * @param topics        Array of topic names to subscribe to
   * @param storageLevel  RDD storage level.
   */
  def createPairedStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[(String, String)] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createPairedStream(jssc.ssc, brokerUrl, topics, storageLevel)
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param jssc               JavaStreamingContext object
   * @param brokerUrl          Url of remote MQTT publisher
   * @param topics             Array of topic names to subscribe to
   * @param storageLevel       RDD storage level.
   * @param clientId           ClientId to use for the mqtt connection
   * @param username           Username for authentication to the mqtt publisher
   * @param password           Password for authentication to the mqtt publisher
   * @param cleanSession       Sets the mqtt cleanSession parameter
   * @param qos                Quality of service to use for the topic subscription
   * @param connectionTimeout  Connection timeout for the mqtt connection
   * @param keepAliveInterval  Keepalive interal for the mqtt connection
   * @param mqttVersion        Version to use for the mqtt connection
   */
  def createPairedStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String],
      storageLevel: StorageLevel,
      clientId: String,
      username: String,
      password: String,
      cleanSession: Boolean,
      qos: Int,
      connectionTimeout: Int,
      keepAliveInterval: Int,
      mqttVersion: Int
    ): JavaReceiverInputDStream[(String, String)] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createPairedStream(jssc.ssc, brokerUrl, topics, storageLevel, Option(clientId),
        Option(username), Option(password), Option(cleanSession), Option(qos),
        Option(connectionTimeout), Option(keepAliveInterval), Option(mqttVersion))
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param jssc               JavaStreamingContext object
   * @param brokerUrl          Url of remote MQTT publisher
   * @param topics             Array of topic names to subscribe to
   * @param clientId           ClientId to use for the mqtt connection
   * @param username           Username for authentication to the mqtt publisher
   * @param password           Password for authentication to the mqtt publisher
   * @param cleanSession       Sets the mqtt cleanSession parameter
   * @param qos                Quality of service to use for the topic subscription
   * @param connectionTimeout  Connection timeout for the mqtt connection
   * @param keepAliveInterval  Keepalive interal for the mqtt connection
   * @param mqttVersion        Version to use for the mqtt connection
   */
  def createPairedStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String],
      clientId: String,
      username: String,
      password: String,
      cleanSession: Boolean,
      qos: Int,
      connectionTimeout: Int,
      keepAliveInterval: Int,
      mqttVersion: Int
    ): JavaReceiverInputDStream[(String, String)] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createPairedStream(jssc.ssc, brokerUrl, topics, StorageLevel.MEMORY_AND_DISK_SER_2,
        Option(clientId), Option(username), Option(password), Option(cleanSession), Option(qos),
        Option(connectionTimeout), Option(keepAliveInterval), Option(mqttVersion))
  }

  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param jssc               JavaStreamingContext object
   * @param brokerUrl          Url of remote MQTT publisher
   * @param topics             Array of topic names to subscribe to
   * @param clientId           ClientId to use for the mqtt connection
   * @param username           Username for authentication to the mqtt publisher
   * @param password           Password for authentication to the mqtt publisher
   * @param cleanSession       Sets the mqtt cleanSession parameter
   */
  def createPairedStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String],
      clientId: String,
      username: String,
      password: String,
      cleanSession: Boolean
    ): JavaReceiverInputDStream[(String, String)] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createPairedStream(jssc.ssc, brokerUrl, topics, StorageLevel.MEMORY_AND_DISK_SER_2,
        Option(clientId), Option(username), Option(password), Option(cleanSession), None,
        None, None, None)
  }
}

/**
 * This is a helper class that wraps the methods in MQTTUtils into more Python-friendly class and
 * function so that it can be easily instantiated and called from Python's MQTTUtils.
 */
private[mqtt] class MQTTUtilsPythonHelper {

  def createStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topic: String,
      storageLevel: StorageLevel
    ): JavaDStream[String] = {
    MQTTUtils.createStream(jssc, brokerUrl, topic, storageLevel)
  }
  def createPairedStream(
      jssc: JavaStreamingContext,
      brokerUrl: String,
      topics: Array[String],
      storageLevel: StorageLevel
    ): JavaDStream[(String, String)] = {
    MQTTUtils.createPairedStream(jssc, brokerUrl, topics, storageLevel)
  }
}
