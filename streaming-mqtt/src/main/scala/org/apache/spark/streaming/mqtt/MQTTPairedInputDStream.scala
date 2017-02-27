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

import java.nio.charset.StandardCharsets

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

/**
 * Input stream that subscribe messages from a Mqtt Broker.
 * Uses eclipse paho as MqttClient http://www.eclipse.org/paho/
 * @param brokerUrl          Url of remote mqtt publisher
 * @param topics             topic name Array to subscribe to
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
private[streaming] class MQTTPairedInputDStream(
    _ssc: StreamingContext,
    brokerUrl: String,
    topics: Array[String],
    storageLevel: StorageLevel,
    clientId: Option[String] = None,
    username: Option[String] = None,
    password: Option[String] = None,
    cleanSession: Option[Boolean] = None,
    qos: Option[Int] = None,
    connectionTimeout: Option[Int] = None,
    keepAliveInterval: Option[Int] = None,
    mqttVersion: Option[Int] = None) extends ReceiverInputDStream[(String, String)](_ssc) {

  private[streaming] override def name: String = s"MQTT stream [$id]"

  def getReceiver(): Receiver[(String, String)] = {
    new MQTTPairReceiver(brokerUrl, topics, storageLevel, clientId, username,
        password, cleanSession, qos, connectionTimeout, keepAliveInterval, mqttVersion)
  }
}

private[streaming] class MQTTPairReceiver(
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
    mqttVersion: Option[Int]) extends Receiver[(String, String)](storageLevel) {

  def onStop() {

  }

  def onStart() {

    // Set up persistence for messages
    val persistence = new MemoryPersistence()

    // Initializing Mqtt Client specifying brokerUrl, clientID and MqttClientPersistance
    val client = new MqttClient(brokerUrl, clientId.getOrElse(MqttClient.generateClientId()),
      persistence)

    // Initialize mqtt parameters
    val mqttConnectionOptions = new MqttConnectOptions()
    if (username.isDefined && password.isDefined) {
      mqttConnectionOptions.setUserName(username.get)
      mqttConnectionOptions.setPassword(password.get.toCharArray)
    }
    mqttConnectionOptions.setCleanSession(cleanSession.getOrElse(true))
    if (connectionTimeout.isDefined) {
      mqttConnectionOptions.setConnectionTimeout(connectionTimeout.get)
    }
    if (keepAliveInterval.isDefined) {
      mqttConnectionOptions.setKeepAliveInterval(keepAliveInterval.get)
    }
    if (mqttVersion.isDefined) {
      mqttConnectionOptions.setMqttVersion(mqttVersion.get)
    }

    // Callback automatically triggers as and when new message arrives on specified topic
    val callback = new MqttCallback() {

      // Handles Mqtt message
      override def messageArrived(topic: String, message: MqttMessage) {
        store((topic, new String(message.getPayload(), StandardCharsets.UTF_8)))
      }

      override def deliveryComplete(token: IMqttDeliveryToken) {
      }

      override def connectionLost(cause: Throwable) {
        restart("Connection lost ", cause)
      }
    }

    // Set up callback for MqttClient. This needs to happen before
    // connecting or subscribing, otherwise messages may be lost
    client.setCallback(callback)

    // Connect to MqttBroker
    client.connect(mqttConnectionOptions)

    // Subscribe to Mqtt topic
    var i = 0;
    val qosArray = Array.ofDim[Int](topics.length);
    for (i <- 0 to qosArray.length -1) {
      qosArray(i) = qos.getOrElse(1);
    }
    client.subscribe(topics, qosArray)

  }
}
