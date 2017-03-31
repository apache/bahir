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

// scalastyle:off
import scalaj.http._

import play.api.libs.json.Json

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.SparkConf

import org.apache.bahir.cloudant.common._
// scalastyle:on

class CloudantReceiver(sparkConf: SparkConf, cloudantParams: Map[String, String])
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {
  lazy val config: CloudantConfig = {
    JsonStoreConfigManager.getConfig(sparkConf, cloudantParams)
      .asInstanceOf[CloudantConfig]
  }

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Cloudant Receiver") {
      override def run() { receive() }
    }.start()
  }

  private def receive(): Unit = {
    val url = config.getContinuousChangesUrl()
    val selector: String = if (config.getSelector() != null) {
      "{\"selector\":" + config.getSelector() + "}"
    } else {
      "{}"
    }

    val clRequest: HttpRequest = config.username match {
      case null =>
        Http(url)
          .postData(selector)
          .timeout(connTimeoutMs = 1000, readTimeoutMs = 0)
          .header("Content-Type", "application/json")
          .header("User-Agent", "spark-cloudant")
      case _ =>
        Http(url)
          .postData(selector)
          .timeout(connTimeoutMs = 1000, readTimeoutMs = 0)
          .header("Content-Type", "application/json")
          .header("User-Agent", "spark-cloudant")
          .auth(config.username, config.password)
    }

    clRequest.exec((code, headers, is) => {
      if (code == 200) {
        scala.io.Source.fromInputStream(is, "utf-8").getLines().foreach(line => {
          if (line.length() > 0) {
            val json = Json.parse(line)
            val jsonDoc = (json \ "doc").get
            val doc = Json.stringify(jsonDoc)
            store(doc)
          }
        })
      } else {
        val status = headers.getOrElse("Status", IndexedSeq.empty)
        val errorMsg = "Error retrieving _changes feed " + config.getDbname() + ": " + status(0)
        reportError(errorMsg, new RuntimeException(errorMsg))
      }
    })
  }

  def onStop(): Unit = {

  }
}
