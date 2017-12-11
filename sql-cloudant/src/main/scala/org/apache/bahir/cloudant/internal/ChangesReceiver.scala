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
package org.apache.bahir.cloudant.internal

import java.io.{BufferedReader, InputStreamReader}

import scalaj.http._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import org.apache.bahir.cloudant.CloudantChangesConfig
import org.apache.bahir.cloudant.common._

class ChangesReceiver(config: CloudantChangesConfig)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Cloudant Receiver") {
      override def run() { receive() }
    }.start()
  }

  private def receive(): Unit = {
    // Get normal _changes url
    val url = config.getChangesReceiverUrl.toString
    val selector: String = {
      "{\"selector\":" + config.getSelector + "}"
    }

    // var count = 0
    val clRequest: HttpRequest = config.username match {
      case null =>
        Http(url)
          .postData(selector)
          .header("Content-Type", "application/json")
          .header("User-Agent", "spark-cloudant")
      case _ =>
        Http(url)
          .postData(selector)
          .header("Content-Type", "application/json")
          .header("User-Agent", "spark-cloudant")
          .auth(config.username, config.password)
    }

    clRequest.exec((code, headers, is) => {
      if (code == 200) {
        var json = new ChangesRow()
        if (is != null) {
          val bufferedReader = new BufferedReader(new InputStreamReader(is))
          while ((json = ChangesRowScanner.readRowFromReader(bufferedReader)) != null) {
            if (!isStopped() && json != null && !json.getDoc.has("_deleted")) {
              store(json.getDoc.toString)
            }
          }
        }
      } else {
        val status = headers.getOrElse("Status", IndexedSeq.empty)
        val errorMsg = "Error retrieving _changes feed " + config.getDbname + ": " + status(0)
        reportError(errorMsg, new CloudantException(errorMsg))
      }
    })
  }

  override def onStop(): Unit = {
  }
}
