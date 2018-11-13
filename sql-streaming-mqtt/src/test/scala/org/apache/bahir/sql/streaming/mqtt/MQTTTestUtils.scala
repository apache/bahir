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

import java.io.File
import java.net.{ServerSocket, URI}
import java.nio.charset.Charset

import scala.collection.mutable

import org.apache.activemq.broker.{BrokerService, TransportConnector}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.{MemoryPersistence, MqttDefaultFilePersistence}

import org.apache.bahir.utils.Logging


class MQTTTestUtils(tempDir: File, port: Int = 0) extends Logging {

  private val brokerHost = "127.0.0.1"
  private val brokerPort: Int = if (port == 0) findFreePort() else port

  private var broker: BrokerService = _
  private var connector: TransportConnector = _

  def brokerUri: String = {
    s"$brokerHost:$brokerPort"
  }

  private def findFreePort() = {
    val s = new ServerSocket(0)
    val port: Int = s.getLocalPort
    s.close()
    port
  }

  def setup(): Unit = {
    broker = new BrokerService()
    broker.setDataDirectoryFile(tempDir)
    connector = new TransportConnector()
    connector.setName("mqtt")
    connector.setUri(new URI("mqtt://" + brokerUri))
    broker.addConnector(connector)
    broker.start()
  }

  def teardown(): Unit = {
    if (broker != null) {
      broker.stop()
    }
    if (connector != null) {
      connector.stop()
      connector = null
    }
    while (!broker.isStopped) {
      Thread.sleep(50)
    }
    broker = null
  }

  def publishData(topic: String, data: String, N: Int = 1): Unit = {
    var client: MqttClient = null
    try {
      val persistence = new MemoryPersistence()
      client = new MqttClient("tcp://" + brokerUri, MqttClient.generateClientId(), persistence)
      client.connect()
      if (client.isConnected) {
        val msgTopic = client.getTopic(topic)
        for (i <- 0 until N) {
          try {
            Thread.sleep(20)
            val message = new MqttMessage(data.getBytes())
            message.setQos(2)
            // message.setId(i) setting id has no effect.
            msgTopic.publish(message)
          } catch {
            case e: MqttException =>
              // wait for Spark sql streaming to consume something from the message queue
              Thread.sleep(50)
              log.warn(s"publish failed", e)
            case x: Throwable => log.warn(s"publish failed $x")
          }
        }
      }
    } finally {
      if (client != null) {
        client.disconnect()
        client.close()
        client = null
      }
    }
  }

  def subscribeData(topic: String, messages: mutable.Map[Int, String]): MqttClient = {
    val client = new MqttClient("tcp://" + brokerUri, MqttClient.generateClientId(), null)
    val callback = new MqttCallbackExtended() {
      override def messageArrived(topic_ : String, message: MqttMessage): Unit = synchronized {
        messages.put(message.getId, new String(message.getPayload, Charset.defaultCharset()))
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
      }

      override def connectionLost(cause: Throwable): Unit = {
      }

      override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
      }
    }
    client.setCallback(callback)
    client.connect()
    client.subscribe(topic)
    client
  }

  def sleepUntil(predicate: => Boolean, timeout: Long): Unit = {
    val deadline = System.currentTimeMillis() + timeout
    while (System.currentTimeMillis() < deadline) {
      Thread.sleep(1000)
      if (predicate) return
    }
  }

}
