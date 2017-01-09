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

import org.apache.activemq.broker.{BrokerService, TransportConnector}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

import org.apache.bahir.utils.Logging


class MQTTTestUtils(tempDir: File, port: Int = 0) extends Logging {

  private val persistenceDir = tempDir.getAbsolutePath
  private val brokerHost = "localhost"
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
      broker = null
    }
    if (connector != null) {
      connector.stop()
      connector = null
    }
  }

  def publishData(topic: String, data: String, N: Int = 1): Unit = {
    var client: MqttClient = null
    try {
      val persistence = new MqttDefaultFilePersistence(persistenceDir)
      client = new MqttClient("tcp://" + brokerUri, MqttClient.generateClientId(), persistence)
      client.connect()
      if (client.isConnected) {
        val msgTopic = client.getTopic(topic)
        for (i <- 0 until N) {
          try {
            Thread.sleep(20)
            val message = new MqttMessage(data.getBytes())
            message.setQos(2)
            message.setRetained(true)
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

}
