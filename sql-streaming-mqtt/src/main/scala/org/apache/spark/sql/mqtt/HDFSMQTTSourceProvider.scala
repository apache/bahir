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

package org.apache.spark.sql.mqtt

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

import org.apache.bahir.sql.streaming.mqtt.{MQTTStreamConstants, MQTTUtils}

/**
 * The provider class for creating MQTT source.
 * This provider throw IllegalArgumentException if  'brokerUrl' or 'topic' parameter
 * is not set in options.
 */
class HDFSMQTTSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
    providerName: String, parameters: Map[String, String]): (String, StructType) = {
    ("hdfs-mqtt", MQTTStreamConstants.SCHEMA_DEFAULT)
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
    schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {

    val parsedResult = MQTTUtils.parseConfigParams(parameters)

    new HdfsBasedMQTTStreamSource(
      sqlContext,
      metadataPath,
      parsedResult._1, // brokerUrl
      parsedResult._2, // clientId
      parsedResult._3, // topic
      parsedResult._5, // mqttConnectionOptions
      parsedResult._6, // qos
      parsedResult._7, // maxBatchMessageNum
      parsedResult._8, // maxBatchMessageSize
      parsedResult._9  // maxRetryNum
    )
  }

  override def shortName(): String = "hdfs-mqtt"
}

object HDFSMQTTSourceProvider {
  val SEP = "##"
}
