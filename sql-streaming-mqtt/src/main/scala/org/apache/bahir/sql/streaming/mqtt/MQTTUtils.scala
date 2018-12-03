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

import org.eclipse.paho.client.mqttv3.{MqttClient, MqttClientPersistence, MqttConnectOptions}
import org.eclipse.paho.client.mqttv3.persist.{MemoryPersistence, MqttDefaultFilePersistence}

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import org.apache.bahir.utils.Logging


private[mqtt] object MQTTUtils extends Logging {
  private[mqtt] def parseConfigParams(config: Map[String, String]):
      (String, String, String, MqttClientPersistence, MqttConnectOptions, Int) = {
    def e(s: String) = new IllegalArgumentException(s)
    val parameters = CaseInsensitiveMap(config)

    val brokerUrl: String = parameters.getOrElse("brokerUrl", parameters.getOrElse("path",
      throw e("Please provide a `brokerUrl` by specifying path or .options(\"brokerUrl\",...)")))

    val persistence: MqttClientPersistence = parameters.get("persistence") match {
      case Some("memory") => new MemoryPersistence()
      case _ => val localStorage: Option[String] = parameters.get("localStorage")
        localStorage match {
          case Some(x) => new MqttDefaultFilePersistence(x)
          case None => new MqttDefaultFilePersistence()
        }
    }

    // if default is subscribe everything, it leads to getting lot unwanted system messages.
    val topic: String = parameters.getOrElse("topic",
      throw e("Please specify a topic, by .options(\"topic\",...)"))

    val clientId: String = parameters.getOrElse("clientId", {
      log.warn("If `clientId` is not set, a random value is picked up." +
        "\nRecovering from failure is not supported in such a case.")
      MqttClient.generateClientId()})

    val username: Option[String] = parameters.get("username")
    val password: Option[String] = parameters.get("password")
    val connectionTimeout: Int = parameters.getOrElse("connectionTimeout",
      MqttConnectOptions.CONNECTION_TIMEOUT_DEFAULT.toString).toInt
    val keepAlive: Int = parameters.getOrElse("keepAlive", MqttConnectOptions
      .KEEP_ALIVE_INTERVAL_DEFAULT.toString).toInt
    val mqttVersion: Int = parameters.getOrElse("mqttVersion", MqttConnectOptions
      .MQTT_VERSION_DEFAULT.toString).toInt
    val cleanSession: Boolean = parameters.getOrElse("cleanSession", "false").toBoolean
    val qos: Int = parameters.getOrElse("QoS", "1").toInt
    val autoReconnect: Boolean = parameters.getOrElse("autoReconnect", "false").toBoolean
    val maxInflight: Int = parameters.getOrElse("maxInflight", "60").toInt

    val mqttConnectOptions: MqttConnectOptions = new MqttConnectOptions()
    mqttConnectOptions.setAutomaticReconnect(autoReconnect)
    mqttConnectOptions.setCleanSession(cleanSession)
    mqttConnectOptions.setConnectionTimeout(connectionTimeout)
    mqttConnectOptions.setKeepAliveInterval(keepAlive)
    mqttConnectOptions.setMqttVersion(mqttVersion)
    mqttConnectOptions.setMaxInflight(maxInflight)
    (username, password) match {
      case (Some(u: String), Some(p: String)) =>
        mqttConnectOptions.setUserName(u)
        mqttConnectOptions.setPassword(p.toCharArray)
      case _ =>
    }

    (brokerUrl, clientId, topic, persistence, mqttConnectOptions, qos)
  }
}
