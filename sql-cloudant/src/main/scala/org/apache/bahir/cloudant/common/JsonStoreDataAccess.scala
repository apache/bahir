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

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Failure, Success}

import scalaj.http.{Http, HttpRequest, HttpResponse}
import ExecutionContext.Implicits.global
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json._

import org.apache.bahir.cloudant.CloudantConfig


class JsonStoreDataAccess (config: CloudantConfig)  {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit lazy val timeout: Long = config.requestTimeout

  def getMany(limit: Int)(implicit columns: Array[String] = null): Seq[String] = {
    if (limit == 0) {
      throw new CloudantException("Database " + config.getDbname +
        " schema sample size is 0!")
    }
    if (limit < -1) {
      throw new CloudantException("Database " + config.getDbname +
        " schema sample size is " + limit + "!")
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

  def getAll[T](url: String)
      (implicit columns: Array[String] = null): Seq[String] = {
    this.getQueryResult[Seq[String]](url, processAll)
  }

  def getIterator(skip: Int, limit: Int, url: String)
      (implicit columns: Array[String] = null,
      postData: String = null): Iterator[String] = {
    val newUrl = config.getSubSetUrl(url, skip, limit, postData != null)
    this.getQueryResult[Iterator[String]](newUrl, processIterator)
  }

  def getTotalRows(url: String, queryUsed: Boolean)
      (implicit postData: String = null): Int = {
      if (queryUsed) config.queryLimit // Query can not retrieve total row now.
      else {
        val totalUrl = config.getTotalUrl(url)
        this.getQueryResult[Int](totalUrl,
          { result => config.getTotalRows(Json.parse(result))})
      }
  }

  private def processAll (result: String)
      (implicit columns: Array[String],
      postData: String = null) = {
    logger.debug(s"processAll:$result, columns:$columns")
    val jsonResult: JsValue = Json.parse(result)
    var rows = config.getRows(jsonResult, postData != null )
    if (config.viewName == null && postData == null) {
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

  private def convert(rec: JsValue)(implicit columns: Array[String]): String = {
    if (columns == null) return Json.stringify(Json.toJson(rec))
    val m = new mutable.HashMap[String, JsValue]()
    for ( x <- columns) {
        val field = JsonUtil.getField(rec, x).getOrElse(JsNull)
        m.put(x, field)
    }
    val result = Json.stringify(Json.toJson(m.toMap))
    logger.debug(s"converted: $result")
    result
  }


  def getChanges(url: String, processResults: (String) => String): String = {
    getQueryResult(url, processResults)
  }

  private def getQueryResult[T]
      (url: String, postProcessor: (String) => T)
      (implicit columns: Array[String] = null,
      postData: String = null) : T = {
    logger.info(s"Loading data from Cloudant using: $url , postData: $postData")

    val clRequest: HttpRequest = getClRequest(url, postData)

    val clResponse: HttpResponse[String] = clRequest.execute()
    if (! clResponse.isSuccess) {
      throw new CloudantException("Database " + config.getDbname +
        " request error: " + clResponse.body)
    }
    val data = postProcessor(clResponse.body)
    logger.debug(s"got result:$data")
    data
  }

  def createDB(): Unit = {
    val url = config.getDbUrl.toString
    val clRequest: HttpRequest = getClRequest(url, null, "PUT")

    val clResponse: HttpResponse[String] = clRequest.execute()
    if (! clResponse.isSuccess) {
      throw new CloudantException("Database " + config.getDbname +
        " create error: " + clResponse.body)
    } else {
      logger.warn(s"Database ${config.getDbname} was created.")
    }
  }


  def getClRequest(url: String, postData: String = null,
                   httpMethod: String = null): HttpRequest = {
    val requestTimeout = config.requestTimeout.toInt
    config.username match {
      case null =>
        if (postData != null) {
          Http(url)
            .postData(postData)
            .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
            .header("Content-Type", "application/json")
            .header("User-Agent", "spark-cloudant")
        } else {
          if (httpMethod != null) {
            Http(url)
              .method(httpMethod)
              .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
              .header("User-Agent", "spark-cloudant")
          } else {
            Http(url)
              .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
              .header("User-Agent", "spark-cloudant")
          }
        }
      case _ =>
        if (postData != null) {
          Http(url)
            .postData(postData)
            .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
            .header("Content-Type", "application/json")
            .header("User-Agent", "spark-cloudant")
            .auth(config.username, config.password)
        } else {
          if (httpMethod != null) {
            Http(url)
              .method(httpMethod)
              .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
              .header("User-Agent", "spark-cloudant")
              .auth(config.username, config.password)
          } else {
            Http(url)
              .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
              .header("User-Agent", "spark-cloudant")
              .auth(config.username, config.password)
          }
        }
    }
  }


  def saveAll(rows: List[String]): Unit = {
    if (rows.isEmpty) return
    val bulkSize = config.bulkSize
    val bulks = rows.grouped(bulkSize).toList
    val totalBulks = bulks.size
    logger.debug(s"total records:${rows.size}=bulkSize:$bulkSize * totalBulks:$totalBulks")

    val futures = bulks.map( bulk => {
      val data = config.getBulkRows(bulk)
      val url = config.getBulkPostUrl.toString
      val clRequest: HttpRequest = getClRequest(url, data)
        Future {
          clRequest.execute()
        }
      }
    )
    // remaining - number of requests remained to succeed
    val remaining = new AtomicInteger(futures.length)
    val p = Promise[HttpResponse[String]]
    futures foreach {
      _ onComplete {
        case Success(clResponse: HttpResponse[String]) =>
          // find if there was error in saving at least one of docs
          val resBody: String = clResponse.body
          val isErr = (resBody contains config.getConflictErrStr) ||
            (resBody contains config.getForbiddenErrStr)
          if (!clResponse.isSuccess || isErr) {
            val e = new CloudantException("Save to database:" + config.getDbname +
                " failed with reason: " + clResponse.body)
            p.tryFailure(e)
          } else if (remaining.decrementAndGet() == 0) {
            // succeed the whole save operation if all requests success
            p.trySuccess(clResponse)
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
