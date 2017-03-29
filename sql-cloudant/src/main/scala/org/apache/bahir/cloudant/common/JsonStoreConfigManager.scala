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
package org.apache.bahir.cloudant.common

import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

import org.apache.bahir.cloudant.CloudantConfig

 object JsonStoreConfigManager {
  val CLOUDANT_CONNECTOR_VERSION = "2.0.0"
  val SCHEMA_FOR_ALL_DOCS_NUM = -1

  private val CLOUDANT_HOST_CONFIG = "cloudant.host"
  private val CLOUDANT_USERNAME_CONFIG = "cloudant.username"
  private val CLOUDANT_PASSWORD_CONFIG = "cloudant.password"
  private val CLOUDANT_PROTOCOL_CONFIG = "cloudant.protocol"


  private val PARTITION_CONFIG = "jsonstore.rdd.partitions"
  private val MAX_IN_PARTITION_CONFIG = "jsonstore.rdd.maxInPartition"
  private val MIN_IN_PARTITION_CONFIG = "jsonstore.rdd.minInPartition"
  private val REQUEST_TIMEOUT_CONFIG = "jsonstore.rdd.requestTimeout"
  private val BULK_SIZE_CONFIG = "bulkSize"
  private val SCHEMA_SAMPLE_SIZE_CONFIG = "schemaSampleSize"
  private val CREATE_DB_ON_SAVE = "createDBOnSave"


  private val configFactory = ConfigFactory.load()

  private val ROOT_CONFIG_NAME = "spark-sql"
  private val rootConfig = configFactory.getConfig(ROOT_CONFIG_NAME)


  /**
   * The sequence of getting configuration
   * 1. "spark."+key in the SparkConf
   *  (as they are treated as the one passed in through spark-submit)
   * 2. key in the parameters, which is set in DF option
   * 3. key in the SparkConf, which is set in SparkConf
   * 4. default in the Config, which is set in the application.conf
   */


  private def getInt(sparkConf: SparkConf, parameters: Map[String, String],
      key: String) : Int = {
    val valueS = parameters.getOrElse(key, null)
    if (sparkConf != null) {
      val default = {
        if (valueS == null) {
          sparkConf.getInt(key, rootConfig.getInt(key))
        } else {
          valueS.toInt
        }
      }
      sparkConf.getInt(s"spark.$key", default)
    } else {
      if (valueS == null) {
        rootConfig.getInt(key)
      } else {
        valueS.toInt
      }
    }
  }

  private def getLong(sparkConf: SparkConf, parameters: Map[String, String],
      key: String) : Long = {
    val valueS = parameters.getOrElse(key, null)
    if (sparkConf != null) {
      val default = {
        if (valueS == null) {
          sparkConf.getLong(key, rootConfig.getLong(key))
        } else {
          valueS.toLong
        }
      }
      sparkConf.getLong(s"spark.$key", default)
    } else {
      if (valueS == null) rootConfig.getLong(key) else valueS.toLong
    }
  }

  private def getString(sparkConf: SparkConf, parameters: Map[String, String],
      key: String) : String = {
    val defaultInConfig = if (rootConfig.hasPath(key)) rootConfig.getString(key) else null
    val valueS = parameters.getOrElse(key, null)
    if (sparkConf != null) {
      val default = {
        if (valueS == null) {
          sparkConf.get(key, defaultInConfig)
        } else {
          valueS
        }
      }
      sparkConf.get(s"spark.$key", default)
    } else {
      if (valueS == null) defaultInConfig else valueS
    }
  }

  private def getBool(sparkConf: SparkConf, parameters: Map[String, String],
      key: String) : Boolean = {
    val valueS = parameters.getOrElse(key, null)
    if (sparkConf != null) {
      val default = {
        if (valueS == null) {
          sparkConf.getBoolean(key, rootConfig.getBoolean(key))
        } else {
          valueS.toBoolean
        }
      }
      sparkConf.getBoolean(s"spark.$key", default)
    } else
    if (valueS == null) {
      rootConfig.getBoolean(key)
    } else {
      valueS.toBoolean
    }
  }



  def getConfig(context: SQLContext, parameters: Map[String, String]): CloudantConfig = {

    val sparkConf = context.sparkContext.getConf

    implicit val total = getInt(sparkConf, parameters, PARTITION_CONFIG)
    implicit val max = getInt(sparkConf, parameters, MAX_IN_PARTITION_CONFIG)
    implicit val min = getInt(sparkConf, parameters, MIN_IN_PARTITION_CONFIG)
    implicit val requestTimeout = getLong(sparkConf, parameters, REQUEST_TIMEOUT_CONFIG)
    implicit val bulkSize = getInt(sparkConf, parameters, BULK_SIZE_CONFIG)
    implicit val schemaSampleSize = getInt(sparkConf, parameters, SCHEMA_SAMPLE_SIZE_CONFIG)
    implicit val createDBOnSave = getBool(sparkConf, parameters, CREATE_DB_ON_SAVE)

    val dbName = parameters.getOrElse("database", parameters.getOrElse("path", null))
    val indexName = parameters.getOrElse("index", null)
    val viewName = parameters.getOrElse("view", null)

    // FIXME: Add logger
    // scalastyle:off println
    println(s"Use connectorVersion=$CLOUDANT_CONNECTOR_VERSION, dbName=$dbName, " +
        s"indexName=$indexName, viewName=$viewName," +
        s"$PARTITION_CONFIG=$total, $MAX_IN_PARTITION_CONFIG=$max," +
        s"$MIN_IN_PARTITION_CONFIG=$min, $REQUEST_TIMEOUT_CONFIG=$requestTimeout," +
        s"$BULK_SIZE_CONFIG=$bulkSize, $SCHEMA_SAMPLE_SIZE_CONFIG=$schemaSampleSize")
    // scalastyle:on println

    val protocol = getString(sparkConf, parameters, CLOUDANT_PROTOCOL_CONFIG)
    val host = getString( sparkConf, parameters, CLOUDANT_HOST_CONFIG)
    val user = getString(sparkConf, parameters, CLOUDANT_USERNAME_CONFIG)
    val passwd = getString(sparkConf, parameters, CLOUDANT_PASSWORD_CONFIG)
    val selector = getString(sparkConf, parameters, "selector")

    if (host != null) {
      new CloudantConfig(protocol, host, dbName, indexName,
        viewName) (user, passwd, total, max, min, requestTimeout, bulkSize,
        schemaSampleSize, createDBOnSave, selector)
    } else {
      throw new RuntimeException("Spark configuration is invalid! " +
        "Please make sure to supply required values for cloudant.host.")
      }
  }

  def getConfig(sparkConf: SparkConf, parameters: Map[String, String]): CloudantConfig = {

    implicit val total = getInt(sparkConf, parameters, PARTITION_CONFIG)
    implicit val max = getInt(sparkConf, parameters, MAX_IN_PARTITION_CONFIG)
    implicit val min = getInt(sparkConf, parameters, MIN_IN_PARTITION_CONFIG)
    implicit val requestTimeout = getLong(sparkConf, parameters, REQUEST_TIMEOUT_CONFIG)
    implicit val bulkSize = getInt(sparkConf, parameters, BULK_SIZE_CONFIG)
    implicit val schemaSampleSize = getInt(sparkConf, parameters, SCHEMA_SAMPLE_SIZE_CONFIG)
    implicit val createDBOnSave = getBool(sparkConf, parameters, CREATE_DB_ON_SAVE)

    val dbName = parameters.getOrElse("database", null)

    // scalastyle:off println
    println(s"Use connectorVersion=$CLOUDANT_CONNECTOR_VERSION, dbName=$dbName, " +
      s"$REQUEST_TIMEOUT_CONFIG=$requestTimeout")
    // scalastyle:on println

    val protocol = getString(sparkConf, parameters, CLOUDANT_PROTOCOL_CONFIG)
    val host = getString( sparkConf, parameters, CLOUDANT_HOST_CONFIG)
    val user = getString(sparkConf, parameters, CLOUDANT_USERNAME_CONFIG)
    val passwd = getString(sparkConf, parameters, CLOUDANT_PASSWORD_CONFIG)
    val selector = getString(sparkConf, parameters, "selector")

    if (host != null) {
      new CloudantConfig(protocol, host, dbName)(user, passwd,
        total, max, min, requestTimeout, bulkSize,
        schemaSampleSize, createDBOnSave, selector)
    } else {
      throw new RuntimeException("Cloudant parameters are invalid!" +
          "Please make sure to supply required values for cloudant.host.")
    }
  }
}
