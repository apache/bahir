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

import java.io.{BufferedReader, InputStreamReader}
import java.util.concurrent.TimeUnit

import okhttp3._

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import org.apache.bahir.cloudant.common._

class CloudantReceiver(sparkConf: SparkConf, cloudantParams: Map[String, String])
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {
  // CloudantChangesConfig requires `_changes` endpoint option
  lazy val config: CloudantChangesConfig = {
    JsonStoreConfigManager.getConfig(sparkConf, cloudantParams
      + ("cloudant.endpoint" -> JsonStoreConfigManager.CHANGES_INDEX)
    ).asInstanceOf[CloudantChangesConfig]
  }

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Cloudant Receiver") {
      override def run() { receive() }
    }.start()
  }

  private def receive(): Unit = {
    val okHttpClient: OkHttpClient = new OkHttpClient.Builder()
      .connectTimeout(5, TimeUnit.SECONDS)
      .readTimeout(60, TimeUnit.SECONDS)
      .build
    val url = config.getChangesReceiverUrl.toString

    val builder = new Request.Builder().url(url)
    if (config.username != null) {
      val credential = Credentials.basic(config.username, config.password)
      builder.header("Authorization", credential)
    }
    if(config.getSelector != null) {
      val jsonType = MediaType.parse("application/json; charset=utf-8")
      val selector = "{\"selector\":" + config.getSelector + "}"
      val selectorBody = RequestBody.create(jsonType, selector)
      builder.post(selectorBody)
    }

    val request = builder.build
    val response = okHttpClient.newCall(request).execute
    val status_code = response.code

    if (status_code == 200) {
      val changesInputStream = response.body.byteStream
      var json = new ChangesRow()
      if (changesInputStream != null) {
        val bufferedReader = new BufferedReader(new InputStreamReader(changesInputStream))
        while ((json = ChangesRowScanner.readRowFromReader(bufferedReader)) != null) {
          if (!isStopped() && json != null && !json.getDoc.has("_deleted")) {
            store(json.getDoc.toString)
          }
        }
      }
    } else {
      val errorMsg = "Error retrieving _changes feed " + config.getDbname + ": " + status_code
      reportError(errorMsg, new CloudantException(errorMsg))
    }
  }

  def onStop(): Unit = {
  }
}
