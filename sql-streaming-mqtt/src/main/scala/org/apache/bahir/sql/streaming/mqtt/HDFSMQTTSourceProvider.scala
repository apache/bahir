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

import java.util.Locale

import org.eclipse.paho.client.mqttv3.{MqttClient, MqttConnectOptions}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import org.apache.bahir.utils.MQTTConfig

/**
 * The provider class for creating MQTT source.
 * This provider throw IllegalArgumentException if  'brokerUrl' or 'topic' parameter
 * is not set in options.
 */
class HDFSMQTTSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
    providerName: String, parameters: Map[String, String]): (String, StructType) = {
    ("mqtt", HDFSMQTTSourceProvider.SCHEMA_DEFAULT)
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
    schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {

    def e(s: String) = new IllegalArgumentException(s)

    val caseInsensitiveParameter = parameters.map{case (key: String, value: String) =>
      key.toLowerCase(Locale.ROOT) -> value
    }

    val brokerUrl: String = getParameterValue(caseInsensitiveParameter, MQTTConfig.brokerUrl, "")

    if (brokerUrl.isEmpty) {
      throw e("Please specify a brokerUrl, by .options(\"brokerUrl\",...)")
    }
    logInfo(s"Using brokerUrl $brokerUrl")

    // if default is subscribe everything, it leads to getting a lot of unwanted system messages.
    val topic: String = getParameterValue(caseInsensitiveParameter, MQTTConfig.topic, "")
    if (topic.isEmpty) {
      throw e("Please specify a topic, by .options(\"topic\",...)")
    }
    logInfo(s"Subscribe topic $topic")

    val clientId: String = getParameterValue(caseInsensitiveParameter, MQTTConfig.clientId, {
      val randomClientId = MqttClient.generateClientId()
      logInfo(s"Using random clientId ${randomClientId}.")
      randomClientId
    })
    logInfo(s"ClientId $clientId")

    val usernameString: String = getParameterValue(caseInsensitiveParameter,
      MQTTConfig.username, "")
    val username: Option[String] = if (usernameString.isEmpty) {
      None
    } else {
      Some(usernameString)
    }

    val passwordString: String = getParameterValue(caseInsensitiveParameter,
      MQTTConfig.password, "")
    val password: Option[String] = if (passwordString.isEmpty) {
      None
    } else {
      Some(passwordString)
    }

    val connectionTimeout: Int = getParameterValue(
      caseInsensitiveParameter,
      MQTTConfig.connectionTimeout,
      MqttConnectOptions.CONNECTION_TIMEOUT_DEFAULT.toString
    ).toInt
    logInfo(s"Set connection timeout $connectionTimeout")

    val keepAlive: Int = getParameterValue(
      caseInsensitiveParameter,
      MQTTConfig.keetAliveInterval,
      MqttConnectOptions.KEEP_ALIVE_INTERVAL_DEFAULT.toString
    ).toInt
    logInfo(s"Set keep alive interval $keepAlive")

    val mqttVersion: Int = getParameterValue(
      caseInsensitiveParameter,
      MQTTConfig.mqttVersion,
      MqttConnectOptions.MQTT_VERSION_DEFAULT.toString
    ).toInt
    logInfo(s"Set mqtt version $mqttVersion")

    val cleanSession: Boolean = getParameterValue(
      caseInsensitiveParameter,
      MQTTConfig.cleanSession,
      "true"
    ).toBoolean
    logInfo(s"Set clean session $cleanSession")

    val qos: Int = getParameterValue(
      caseInsensitiveParameter,
      MQTTConfig.qos,
      "0"
    ).toInt
    logInfo(s"Set qos $qos")

    val maxBatchMessageNum = getParameterValue(
      caseInsensitiveParameter,
      MQTTConfig.maxBatchMessageNum, s"${Long.MaxValue}"
    ).toLong
    logInfo(s"Control max message number in one batch $maxBatchMessageNum")

    val maxBatchMessageSize = getParameterValue(
      caseInsensitiveParameter,
      MQTTConfig.maxBatchMessageSize, s"${Long.MaxValue}"
    ).toLong
    logInfo(s"Control max message content size in one batch $maxBatchMessageSize")

    val maxRetryNumber = getParameterValue(
      caseInsensitiveParameter,
      MQTTConfig.maxRetryNumber, "3"
    ).toInt
    logInfo(s"Set max retry number $maxRetryNumber")

    val mqttConnectOptions: MqttConnectOptions = new MqttConnectOptions()
    mqttConnectOptions.setAutomaticReconnect(true)
    mqttConnectOptions.setCleanSession(cleanSession)
    mqttConnectOptions.setConnectionTimeout(connectionTimeout)
    mqttConnectOptions.setKeepAliveInterval(keepAlive)
    mqttConnectOptions.setMqttVersion(mqttVersion)
    (username, password) match {
      case (Some(u: String), Some(p: String)) =>
        mqttConnectOptions.setUserName(u)
        mqttConnectOptions.setPassword(p.toCharArray)
      case _ =>
    }

    new HdfsBasedMQTTStreamSource(
      sqlContext,
      metadataPath,
      brokerUrl,
      topic,
      clientId,
      mqttConnectOptions,
      qos,
      maxBatchMessageNum,
      maxBatchMessageSize,
      maxRetryNumber)
  }

  override def shortName(): String = "hdfs-mqtt"

  private def getParameterValue(
      parameters: Map[String, String],
      parameterName: String,
      defaultValue: String): String = {

    parameters.getOrElse(parameterName.toLowerCase(Locale.ROOT), defaultValue)
  }
}

object HDFSMQTTSourceProvider {
  val SCHEMA_DEFAULT = StructType(
    StructField("value", StringType) :: StructField("timestamp", LongType) :: Nil)
  val SEP = "##"
}
