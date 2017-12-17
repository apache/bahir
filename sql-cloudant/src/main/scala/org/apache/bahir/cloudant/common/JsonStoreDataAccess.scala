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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Failure, Success}

import ExecutionContext.Implicits.global
import com.cloudant.client.api.model.Response
import com.cloudant.http.HttpConnection
import com.google.gson.{Gson, JsonElement, JsonObject}
import org.slf4j.{Logger, LoggerFactory}

import org.apache.bahir.cloudant.CloudantConfig

class JsonStoreDataAccess (config: CloudantConfig)  {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit lazy val timeout: Long = config.requestTimeout

  def getMany(limit: Int)(implicit columns: Array[String] = null): Seq[String] = {
    if (limit == 0 || limit < -1) {
      throw new CloudantException("Schema size '" + limit + "' is not valid.")
    }
    var r = this.getQueryResult[Seq[String]](config.getUrl(limit), processAll)
    if (r.isEmpty) {
      r = this.getQueryResult[Seq[String]](config.getUrl(limit, excludeDDoc = true),
        processAll)
    }
    if (r.isEmpty) {
      throw new CloudantException("Database " + config.getDbname +
        " doesn't have any non-design documents!")
    } else {
      r
    }
  }

  def getIterator(skip: Int, limit: Int, url: String)
      (implicit columns: Array[String] = null,
      postData: String = null): Iterator[String] = {
    logger.info(s"Loading data from Cloudant using: $url , postData: $postData")
    val newUrl = config.getSubSetUrl(url, skip, limit, postData != null)
    this.getQueryResult[Iterator[String]](newUrl, processIterator)
  }

  def getTotalRows(url: String, queryUsed: Boolean)
                  (implicit postData: String = null): Int = {
    if (queryUsed) config.queryLimit // Query can not retrieve total row now.
    else {
      val totalUrl = config.getTotalUrl(url)
      this.getQueryResult[Int](totalUrl,
        { result => config.getResultCount(result)})
    }
  }

  private def processAll(result: String)
      (implicit columns: Array[String],
      postData: String = null) = {
    logger.debug(s"processAll:$result, columns:$columns")
    var rows = config.getRows(result, postData != null)
    if (config.viewPath == null && postData == null) {
      // filter design docs
      rows = rows.filter(r => FilterDDocs.filter(r))
    }
    rows.map(r => convert(r))
  }

  private def processIterator (result: String)
    (implicit columns: Array[String],
      postData: String = null): Iterator[String] = {
    processAll(result).iterator
  }

  private def convert(rec: JsonElement)(implicit columns: Array[String]): String = {
    if (columns == null) {
      rec.getAsJsonObject.toString
    } else {
      val jsonObject = new JsonObject
      for (x <- columns) {
        val field = JsonUtil.getField(rec, x).orNull
        jsonObject.add(x, field)
      }
      val result = jsonObject.toString
      logger.debug(s"converted: $result")
      result
    }
  }

  private def getQueryResult[T]
  (url: String, postProcessor: (String) => T)
  (implicit columns: Array[String] = null,
   postData: String = null) : T = {
    logger.info(s"Loading data from Cloudant using: $url , postData: $postData")

    val clRequest: HttpConnection = config.executeRequest(url, postData)

    val clResponse: HttpConnection = clRequest.execute()
    if (clResponse.getConnection.getResponseCode != 200) {
      throw new CloudantException("Database " + config.getDbname +
        " request error: " + clResponse.responseAsString)
    }
    val data = postProcessor(clResponse.responseAsString)
    logger.debug(s"got result:$data")
    data
  }

  def createDB(): Unit = {
    config.getClient.createDB(config.getDbname)
  }

  def saveAll(rows: List[String]): Unit = {
    if (rows.isEmpty) return
    val bulkSize = config.bulkSize
    val bulks = rows.grouped(bulkSize).toList
    val totalBulks = bulks.size
    logger.debug(s"total records:${rows.size}=bulkSize:$bulkSize * totalBulks:$totalBulks")

    val futures = bulks.map( bulk => {
      val jsonData = config.getBulkRows(bulk)
        Future {
          config.getDatabase.bulk(jsonData.asJava)
        }
      }
    )
    // remaining - number of requests remained to succeed
    val remaining = new AtomicInteger(futures.length)
    val p = Promise[java.util.List[Response]]
    futures foreach {
      _ onComplete {
        case Success(clResponses) =>
          if (clResponses contains config.getConflictErrStr) {
            val e = new CloudantException("Save to database:" + config.getDbname +
              " failed with reason: " + config.getConflictErrStr)
            p.tryFailure(e)
          } else if (clResponses contains config.getForbiddenErrStr) {
            val e = new CloudantException("Save to database:" + config.getDbname +
              " failed with reason: " + config.getForbiddenErrStr)
            p.tryFailure(e)
          } else if (remaining.decrementAndGet() == 0) {
            // succeed the whole save operation if all requests success
            p.trySuccess(clResponses)
          }
        // if a least one save request fails - fail the whole save operation
        case Failure(e) =>
          p.tryFailure(e)
      }
    }

    val mainFtr = p.future
    mainFtr onSuccess {
      case clResponsesList =>
        logger.warn(s"Saved total ${rows.length} " +
          s"with bulkSize $bulkSize " +
          s"for database: ${config.getDbname}")
    }
    mainFtr onFailure  {
      case e =>
        throw new CloudantException("Save to database:" + config.getDbname +
          " failed with reason: " + e.getMessage)
    }
    Await.result(mainFtr, (config.requestTimeout * totalBulks).millis) // scalastyle:ignore
  }

}
