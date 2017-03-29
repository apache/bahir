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

import scala.collection.mutable.HashMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Failure, Success}

import scalaj.http.{Http, HttpRequest, HttpResponse}
import ExecutionContext.Implicits.global
import org.slf4j.LoggerFactory
import play.api.libs.json._

import org.apache.spark.sql.sources._

import org.apache.bahir.cloudant.CloudantConfig
import org.apache.bahir.cloudant.common._


class JsonStoreDataAccess (config: CloudantConfig)  {
  lazy val logger = LoggerFactory.getLogger(getClass)
  implicit lazy val timeout = config.requestTimeout

  def getOne()( implicit columns: Array[String] = null): Seq[String] = {
    var r = this.getQueryResult[Seq[String]](config.getOneUrlExcludeDDoc1(), processAll)
    if (r.size == 0 ) {
      r = this.getQueryResult[Seq[String]](config.getOneUrlExcludeDDoc2(), processAll)
    }
    if (r.size == 0) {
      throw new RuntimeException("Database " + config.getDbname() +
        " doesn't have any non-design documents!")
    } else {
      r
    }
  }

  def getMany(limit: Int)(implicit columns: Array[String] = null): Seq[String] = {
    if (limit == 0) {
      throw new RuntimeException("Database " + config.getDbname() +
        " schema sample size is 0!")
    }
    if (limit < -1) {
      throw new RuntimeException("Database " + config.getDbname() +
        " schema sample size is " + limit + "!")
    }
    var r = this.getQueryResult[Seq[String]](config.getAllDocsUrl(limit), processAll)
    if (r.size == 0) {
      r = this.getQueryResult[Seq[String]](config.getAllDocsUrlExcludeDDoc(limit), processAll)
    }
    if (r.size == 0) {
      throw new RuntimeException("Database " + config.getDbname() +
        " doesn't have any non-design documents!")
    } else {
      r
    }
  }

  def getAll[T](url: String)
      (implicit columns: Array[String] = null,
      attrToFilters: Map[String, Array[Filter]] = null): Seq[String] = {
    this.getQueryResult[Seq[String]](url, processAll)
  }

  def getIterator(skip: Int, limit: Int, url: String)
      (implicit columns: Array[String] = null,
      attrToFilters: Map[String, Array[Filter]] = null): Iterator[String] = {
    implicit def convertSkip(skip: Int): String = {
      val url = config.getLastUrl(skip)
      if (url == null) {
        skip.toString()
      } else {
        this.getQueryResult[String](url,
          { result => config.getLastNum(Json.parse(result)).as[JsString].value})
      }
    }
    val newUrl = config.getSubSetUrl(url, skip, limit)
    this.getQueryResult[Iterator[String]](newUrl, processIterator)
  }

  def getTotalRows(url: String): Int = {
    val totalUrl = config.getTotalUrl(url)
    this.getQueryResult[Int](totalUrl,
        { result => config.getTotalRows(Json.parse(result))})
  }

  private def processAll (result: String)
      (implicit columns: Array[String],
      attrToFilters: Map[String, Array[Filter]] = null) = {
    logger.debug(s"processAll columns:$columns, attrToFilters:$attrToFilters")
    val jsonResult: JsValue = Json.parse(result)
    var rows = config.getRows(jsonResult)
    if (config.viewName == null) {
      // filter design docs
      rows = rows.filter(r => FilterDDocs.filter(r))
    }
    rows.map(r => convert(r))
  }

  private def processIterator (result: String)
    (implicit columns: Array[String],
    attrToFilters: Map[String, Array[Filter]] = null): Iterator[String] = {
    processAll(result).iterator
  }

  private def convert(rec: JsValue)(implicit columns: Array[String]): String = {
    if (columns == null) return Json.stringify(Json.toJson(rec))
    val m = new HashMap[String, JsValue]()
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
      attrToFilters: Map[String, Array[Filter]] = null) : T = {
    logger.warn("Loading data from Cloudant using query: " + url)
    val requestTimeout = config.requestTimeout.toInt
    val clRequest: HttpRequest = config.username match {
      case null =>
        Http(url)
            .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
            .header("User-Agent", "spark-cloudant")
      case _ =>
        Http(url)
            .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
            .header("User-Agent", "spark-cloudant")
            .auth(config.username, config.password)
    }

    val clResponse: HttpResponse[String] = clRequest.execute()
    if (! clResponse.isSuccess) {
      throw new RuntimeException("Database " + config.getDbname() +
          " request error: " + clResponse.body)
    }
    val data = postProcessor(clResponse.body)
    logger.debug(s"got result:$data")
    data
  }


  def createDB(): Unit = {
    val url = config.getDbUrl()
    val requestTimeout = config.requestTimeout.toInt
    val clRequest: HttpRequest = config.username match {
      case null =>
        Http(url)
          .method("put")
          .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
          .header("User-Agent", "spark-cloudant")
      case _ =>
        Http(url)
          .method("put")
          .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
          .header("User-Agent", "spark-cloudant")
          .auth(config.username, config.password)
    }

    val clResponse: HttpResponse[String] = clRequest.execute()
    if (! clResponse.isSuccess) {
      throw new RuntimeException("Database " + config.getDbname() +
        " create error: " + clResponse.body)
    } else {
      logger.warn(s"Database ${config.getDbname()} was created.")
    }
  }


  def getClPostRequest(data: String): HttpRequest = {
    val url = config.getBulkPostUrl()
    val requestTimeout = config.requestTimeout.toInt
    config.username match {
      case null =>
        Http(url)
          .postData(data)
          .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
          .header("Content-Type", "application/json")
          .header("User-Agent", "spark-cloudant")
      case _ =>
        Http(url)
          .postData(data)
          .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
          .header("Content-Type", "application/json")
          .header("User-Agent", "spark-cloudant")
          .auth(config.username, config.password)
    }
  }


  def saveAll(rows: List[String]): Unit = {
    if (rows.size == 0) return
    val bulkSize = config.bulkSize
    val bulks = rows.grouped(bulkSize).toList
    val totalBulks = bulks.size
    logger.debug(s"total records:${rows.size}=bulkSize:$bulkSize * totalBulks:$totalBulks")

    val futures = bulks.map( bulk => {
        val data = config.getBulkRows(bulk)
        val clRequest: HttpRequest = getClPostRequest(data)
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
          val isErr = (resBody contains config.getConflictErrStr()) ||
            (resBody contains config.getForbiddenErrStr())
          if (!(clResponse.isSuccess) || isErr) {
            val e = new RuntimeException("Save to database:" + config.getDbname() +
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
          s"for database: ${config.getDbname()}")
    }
    mainFtr onFailure  {
      case e =>
        throw new RuntimeException("Save to database:" + config.getDbname() +
          " failed with reason: " + e.getMessage)
    }
    Await.result(mainFtr, (config.requestTimeout * totalBulks).millis) // scalastyle:ignore
  }

}
