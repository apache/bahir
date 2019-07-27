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
import java.nio.file.Files
import java.security.{KeyStore, SecureRandom}
import java.util.Properties
import javax.net.ssl.KeyManagerFactory

import scala.collection.mutable

import org.apache.activemq.broker.{BrokerService, SslBrokerService, SslContext, TransportConnector}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.apache.bahir.utils.Logging


class MQTTTestUtils(tempDir: File, port: Int = 0, ssl: Boolean = false) extends Logging {

  private val brokerHost = "127.0.0.1"
  private val brokerPort: Int = if (port == 0) findFreePort() else port
  val serverKeyStore = new File("src/test/resources/keystore.jks")
  val serverKeyStorePassword = "changeit"
  val clientTrustStore = new File("src/test/resources/truststore.jks")
  val clientTrustStorePassword = "changeit"

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
    broker = if (ssl) new SslBrokerService() else new BrokerService()
    broker.setDataDirectoryFile(tempDir)
    val protocol = if (ssl) "mqtt+ssl" else "mqtt"
    if (ssl) {
      val keyStore = KeyStore.getInstance("JKS")
      keyStore.load(Files.newInputStream(serverKeyStore.toPath), serverKeyStorePassword.toCharArray)
      val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keyManagerFactory.init(keyStore, serverKeyStorePassword.toCharArray)
      broker.setSslContext(
        new SslContext(keyManagerFactory.getKeyManagers, null, new SecureRandom())
      )
    }
    connector = new TransportConnector()
    connector.setName("mqtt")
    connector.setUri(new URI(protocol + "://" + brokerUri))
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
      client = connectToServer(new MemoryPersistence(), null)
      if (client.isConnected) {
        val msgTopic = client.getTopic(topic)
        for (_ <- 0 until N) {
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
    val client = connectToServer(null, callback)
    client.subscribe(topic)
    client
  }

  def connectToServer(
    persistence: MqttClientPersistence, callback: MqttCallbackExtended
  ): MqttClient = {
    val protocol = if (ssl) "ssl" else "tcp"
    val client = new MqttClient(
      protocol + "://" + brokerUri, MqttClient.generateClientId(), persistence
    )
    val connectOptions: MqttConnectOptions = new MqttConnectOptions()
    if (ssl) {
      val sslProperties = new Properties()
      sslProperties.setProperty("com.ibm.ssl.trustStore", clientTrustStore.getAbsolutePath)
      sslProperties.setProperty("com.ibm.ssl.trustStoreType", "JKS")
      sslProperties.setProperty("com.ibm.ssl.trustStorePassword", clientTrustStorePassword)
      connectOptions.setSSLProperties(sslProperties)
    }
    if (callback != null) {
      client.setCallback(callback)
    }
    client.connect(connectOptions)
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
