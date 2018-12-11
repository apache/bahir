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

import java.util.concurrent.{ExecutionException, TimeUnit}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.eclipse.paho.client.mqttv3.{IMqttDeliveryToken, MqttCallbackExtended, MqttClient, MqttClientPersistence, MqttException, MqttMessage}
import scala.util.control.NonFatal

import org.apache.spark.SparkEnv

import org.apache.bahir.utils.Logging
import org.apache.bahir.utils.Retry


private[mqtt] object CachedMQTTClient extends Logging {
  private lazy val cacheExpireTimeout: Long =
    SparkEnv.get.conf.getTimeAsMs("spark.mqtt.connection.cache.timeout", "10m")
  private lazy val connectAttempts: Int =
    SparkEnv.get.conf.getInt("spark.mqtt.client.connect.attempts", -1)
  private lazy val connectBackoff: Long =
    SparkEnv.get.conf.getTimeAsMs("spark.mqtt.client.connect.backoff", "5s")

  private val cacheLoader = new CacheLoader[Seq[(String, String)],
      (MqttClient, MqttClientPersistence)] {
    override def load(config: Seq[(String, String)]): (MqttClient, MqttClientPersistence) = {
      log.debug(s"Creating new MQTT client with params: $config")
      createMqttClient(Map(config.map(s => s._1 -> s._2): _*))
    }
  }

  private val removalListener = new RemovalListener[Seq[(String, String)],
      (MqttClient, MqttClientPersistence)]() {
    override def onRemoval(notification: RemovalNotification[Seq[(String, String)],
      (MqttClient, MqttClientPersistence)]): Unit = {
      val params: Seq[(String, String)] = notification.getKey
      val client: MqttClient = notification.getValue._1
      val persistence: MqttClientPersistence = notification.getValue._2
      log.debug(s"Evicting MQTT client $client params: $params, due to ${notification.getCause}")
      closeMqttClient(params, client, persistence)
    }
  }

  private lazy val cache: LoadingCache[Seq[(String, String)],
      (MqttClient, MqttClientPersistence)] =
    CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[Seq[(String, String)], (MqttClient, MqttClientPersistence)](cacheLoader)

  private def createMqttClient(config: Map[String, String]):
      (MqttClient, MqttClientPersistence) = {
    val (brokerUrl, clientId, _, persistence, mqttConnectOptions, _, _, _, _) =
      MQTTUtils.parseConfigParams(config)
    val client = new MqttClient(brokerUrl, clientId, persistence)
    val callback = new MqttCallbackExtended() {
      override def messageArrived(topic : String, message: MqttMessage): Unit = synchronized {
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
      }

      override def connectionLost(cause: Throwable): Unit = {
        log.warn("Connection to mqtt server lost.", cause)
      }

      override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
        log.info(s"Connect complete $serverURI. Is it a reconnect?: $reconnect")
      }
    }
    client.setCallback(callback)
    Retry(connectAttempts, connectBackoff, classOf[MqttException]) {
      client.connect(mqttConnectOptions)
    }
    (client, persistence)
  }

  private def closeMqttClient(params: Seq[(String, String)],
      client: MqttClient, persistence: MqttClientPersistence): Unit = {
    try {
      if (client.isConnected) {
        client.disconnect()
      }
      try {
        persistence.close()
      } catch {
        case NonFatal(e) => log.warn(
          s"Error while closing MQTT persistent store ${e.getMessage}", e
        )
      }
      client.close()
    } catch {
      case NonFatal(e) => log.warn(s"Error while closing MQTT client ${e.getMessage}", e)
    }
  }

  private[mqtt] def getOrCreate(parameters: Map[String, String]): MqttClient = {
    try {
      cache.get(mapToSeq(parameters))._1
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null => throw e.getCause
    }
  }

  private[mqtt] def close(parameters: Map[String, String]): Unit = {
    cache.invalidate(mapToSeq(parameters))
  }

  private[mqtt] def clear(): Unit = {
    log.debug("Cleaning MQTT client cache")
    cache.invalidateAll()
  }

  private def mapToSeq(parameters: Map[String, String]): Seq[(String, String)] = {
    parameters.toSeq.sortBy(x => x._1)
  }
}
