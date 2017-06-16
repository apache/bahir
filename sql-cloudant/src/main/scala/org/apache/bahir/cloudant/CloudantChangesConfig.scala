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
package org.apache.bahir.cloudant

import org.apache.spark.storage.StorageLevel

import org.apache.bahir.cloudant.common.JsonStoreConfigManager

class CloudantChangesConfig(protocol: String, host: String, dbName: String,
                            indexName: String = null, viewName: String = null)
                           (username: String, password: String, partitions: Int,
                            maxInPartition: Int, minInPartition: Int, requestTimeout: Long,
                            bulkSize: Int, schemaSampleSize: Int,
                            createDBOnSave: Boolean, endpoint: String, selector: String,
                            timeout: Int, storageLevel: StorageLevel, useQuery: Boolean,
                            queryLimit: Int)
  extends CloudantConfig(protocol, host, dbName, indexName, viewName)(username, password,
    partitions, maxInPartition, minInPartition, requestTimeout, bulkSize, schemaSampleSize,
    createDBOnSave, endpoint, useQuery, queryLimit) {

  override val defaultIndex: String = endpoint

  def getSelector : String = {
    if (selector != null && !selector.isEmpty) {
      selector
    } else {
      // Exclude design docs and deleted=true docs
      "{ \"_id\": { \"$regex\": \"^(?!_design/)\" }, " +
        "\"_deleted\": { \"$exists\": false } }"
    }
  }

  /*
   * Storage level when persisting RDDs during streaming.
   * See https://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence for
   * more details.
   * See [[org.apache.spark.storage.StorageLevel]] for all defined storage level options.
   */
  def getStorageLevelForStreaming : StorageLevel = {
    if (storageLevel == null) {
      StorageLevel.MEMORY_ONLY
    } else {
      storageLevel
    }
  }

  def getContinuousChangesUrl: String = {
    var url = dbUrl + "/" + defaultIndex + "?include_docs=true&feed=continuous&heartbeat=3000"
    if (getSelector != null) {
      url = url + "&filter=_selector"
    }
    url
  }

  def getChangesReceiverUrl: String = {
    var url = dbUrl + "/" + defaultIndex + "?include_docs=true&feed=continuous&timeout=" + timeout
    if (getSelector != null) {
      url = url + "&filter=_selector"
    }
    url
  }

  // Use _all_docs endpoint for getting the total number of docs
  def getTotalUrl: String = {
    dbUrl + "/" + JsonStoreConfigManager.ALL_DOCS_INDEX
  }
}
