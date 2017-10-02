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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import org.apache.bahir.cloudant.{CloudantChangesConfig, CloudantConfig}

object JsonStoreConfigManager {
  val CLOUDANT_CONNECTOR_VERSION = "2.0.0"
  val ALLDOCS_OR_CHANGES_LIMIT: Int = -1
  val CHANGES_INDEX = "_changes"
  val ALL_DOCS_INDEX = "_all_docs"

  private val CLOUDANT_HOST_CONFIG = "cloudant.host"
  private val CLOUDANT_USERNAME_CONFIG = "cloudant.username"
  private val CLOUDANT_PASSWORD_CONFIG = "cloudant.password"
  private val CLOUDANT_PROTOCOL_CONFIG = "cloudant.protocol"
  private val CLOUDANT_API_ENDPOINT = "cloudant.endpoint"
  private val CLOUDANT_STREAMING_BATCH_INTERVAL = "cloudant.batchInterval"
  private val STORAGE_LEVEL_FOR_CHANGES_INDEX = "cloudant.storageLevel"
  private val CLOUDANT_CHANGES_TIMEOUT = "cloudant.timeout"
  private val USE_QUERY_CONFIG = "cloudant.useQuery"
  private val QUERY_LIMIT_CONFIG = "cloudant.queryLimit"
  private val FILTER_SELECTOR = "selector"

  private val PARTITION_CONFIG = "jsonstore.rdd.partitions"
  private val MAX_IN_PARTITION_CONFIG = "jsonstore.rdd.maxInPartition"
  private val MIN_IN_PARTITION_CONFIG = "jsonstore.rdd.minInPartition"
  private val REQUEST_TIMEOUT_CONFIG = "jsonstore.rdd.requestTimeout"
  private val BULK_SIZE_CONFIG = "bulkSize"
  private val SCHEMA_SAMPLE_SIZE_CONFIG = "schemaSampleSize"
  private val CREATE_DB_ON_SAVE_CONFIG = "createDBOnSave"

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
      val sparkDefault = sparkConf.get(s"spark.$key", default)
      if(sparkDefault != null && sparkDefault.isEmpty) {
        throw new CloudantException(s"spark.$key parameter is empty. " +
          s"Please supply the required value.")
      } else {
        sparkDefault
      }
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

  private def getStorageLevel(sparkConf: SparkConf, parameters: Map[String, String],
                        key: String) : StorageLevel = {
    val storageValue = getString(sparkConf, parameters, key)
    StorageLevel.fromString(storageValue)
  }

  def getConfig(context: SQLContext, parameters: Map[String, String]): CloudantConfig = {

    val sparkConf = context.sparkContext.getConf
    getConfig(sparkConf, parameters)
  }

  def getConfig (sparkConf: SparkConf, parameters: Map[String, String]): CloudantConfig = {

    implicit val total = getInt(sparkConf, parameters, PARTITION_CONFIG)
    implicit val max = getInt(sparkConf, parameters, MAX_IN_PARTITION_CONFIG)
    implicit val min = getInt(sparkConf, parameters, MIN_IN_PARTITION_CONFIG)
    implicit val requestTimeout = getLong(sparkConf, parameters, REQUEST_TIMEOUT_CONFIG)
    implicit val bulkSize = getInt(sparkConf, parameters, BULK_SIZE_CONFIG)
    implicit val schemaSampleSize = getInt(sparkConf, parameters, SCHEMA_SAMPLE_SIZE_CONFIG)
    implicit val createDBOnSave = getBool(sparkConf, parameters, CREATE_DB_ON_SAVE_CONFIG)
    implicit val endpoint = getString(sparkConf, parameters, CLOUDANT_API_ENDPOINT)
    implicit val selector = getString(sparkConf, parameters, FILTER_SELECTOR)
    implicit val storageLevel = getStorageLevel(
      sparkConf, parameters, STORAGE_LEVEL_FOR_CHANGES_INDEX)
    implicit val timeout = getInt(sparkConf, parameters, CLOUDANT_CHANGES_TIMEOUT)
    implicit val batchInterval = getInt(sparkConf, parameters, CLOUDANT_STREAMING_BATCH_INTERVAL)

    implicit val useQuery = getBool(sparkConf, parameters, USE_QUERY_CONFIG)
    implicit val queryLimit = getInt(sparkConf, parameters, QUERY_LIMIT_CONFIG)

    val dbName = parameters.getOrElse("database", parameters.getOrElse("path",
      throw new CloudantException(s"Cloudant database name is empty. " +
        s"Please supply the required value.")))
    val indexName = parameters.getOrElse("index", null)
    val viewName = parameters.getOrElse("view", null)

    val protocol = getString(sparkConf, parameters, CLOUDANT_PROTOCOL_CONFIG)
    val host = getString( sparkConf, parameters, CLOUDANT_HOST_CONFIG)
    val user = getString(sparkConf, parameters, CLOUDANT_USERNAME_CONFIG)
    val passwd = getString(sparkConf, parameters, CLOUDANT_PASSWORD_CONFIG)

    if (endpoint == ALL_DOCS_INDEX) {
      new CloudantConfig(protocol, host, dbName, indexName,
        viewName) (user, passwd, total, max, min, requestTimeout, bulkSize,
        schemaSampleSize, createDBOnSave, endpoint, useQuery,
        queryLimit)
    } else if (endpoint == CHANGES_INDEX) {
      new CloudantChangesConfig(protocol, host, dbName, indexName,
        viewName) (user, passwd, total, max, min, requestTimeout,
        bulkSize, schemaSampleSize, createDBOnSave, endpoint, selector,
        timeout, storageLevel, useQuery, queryLimit, batchInterval)
    } else {
      throw new CloudantException(s"spark.$CLOUDANT_API_ENDPOINT parameter " +
        s"is invalid. Please supply the valid option '" + ALL_DOCS_INDEX + "' or '" +
        CHANGES_INDEX + "'.")
    }
  }
}
