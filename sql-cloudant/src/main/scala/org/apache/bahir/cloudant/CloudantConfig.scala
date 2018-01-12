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

import java.net.{URL, URLEncoder}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.io.File

import com.cloudant.client.api.{ClientBuilder, CloudantClient, Database}
import com.cloudant.client.api.model.SearchResult
import com.cloudant.client.api.views._
import com.cloudant.http.{Http, HttpConnection}
import com.cloudant.http.interceptors.Replay429Interceptor
import com.google.gson.{JsonObject, JsonParser}

import org.apache.bahir.cloudant.common._
import org.apache.bahir.cloudant.common.JsonUtil.JsonConverter

/*
* Only allow one field pushdown now
* as the filter today does not tell how to link the filters out And v.s. Or
*/

class CloudantConfig(val protocol: String, val host: String,
                     val dbName: String, val indexPath: String, val viewPath: String)
                    (implicit val username: String, val password: String,
                     val partitions: Int, val maxInPartition: Int, val minInPartition: Int,
                     val requestTimeout: Long, val bulkSize: Int, val schemaSampleSize: Int,
                     val createDBOnSave: Boolean, val endpoint: String,
                     val useQuery: Boolean = false, val queryLimit: Int,
                     val numberOfRetries: Int)
  extends Serializable {

  @transient private lazy val client: CloudantClient = ClientBuilder
    .url(getClientUrl)
    .username(username)
    .password(password)
    .interceptors(new Replay429Interceptor(numberOfRetries, 250L))
    .build
  @transient private lazy val database: Database = client.database(dbName, false)
  lazy val dbUrl: String = {protocol + "://" + host + "/" + dbName}

  val pkField = "_id"
  val defaultIndex: String = endpoint
  val default_filter: String = "*:*"

  def executeRequest(stringUrl: String, postData: String = null): HttpConnection = {
    val url = new URL(stringUrl)
    if(postData != null) {
      val conn = Http.POST(url, "application/json")
      conn.setRequestBody(postData)
      conn.requestProperties.put("User-Agent", "spark-cloudant")
      client.executeRequest(conn)
    } else {
      val conn = Http.GET(url)
      conn.requestProperties.put("User-Agent", "spark-cloudant")
      client.executeRequest(conn)
    }
  }

  def getClient: CloudantClient = {
    client
  }

  def getDatabase: Database = {
    database
  }

  def getSchemaSampleSize: Int = {
    schemaSampleSize
  }

  def getCreateDBonSave: Boolean = {
    createDBOnSave
  }

  def getClientUrl: URL = {
    new URL(protocol + "://" + host)
  }

  def getLastNum(result: JsonObject): JsonObject = result.get("last_seq").getAsJsonObject

  /* Url containing limit for docs in a Cloudant database.
  * If a view is not defined, use the _all_docs endpoint.
  * @return url with one doc limit for retrieving total doc count
  */
  def getUrl(limit: Int, excludeDDoc: Boolean = false): String = {
    if (viewPath == null) {
      val baseUrl = {
        dbUrl + "/_all_docs?include_docs=true"
      }
      if (limit == JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
        baseUrl
      } else {
        baseUrl + "&limit=" + limit
      }
    } else {
      if (limit == JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
        dbUrl + "/" + viewPath
      } else {
        dbUrl + "/" + viewPath + "?limit=" + limit
      }
    }
  }

  /* Total count of documents in a Cloudant database.
  *
  * @return total doc count number
  */
  def getTotalUrl(url: String): String = {
    if (url.contains('?')) {
      url + "&limit=1"
    } else {
      url + "?limit=1"
    }
  }

  def getDbname: String = {
    dbName
  }

  def queryEnabled: Boolean = {
    useQuery && indexPath == null && viewPath == null
  }

  def allowPartition(queryUsed: Boolean): Boolean = {indexPath == null && !queryUsed}

  def getRangeUrl(field: String = null, start: Any = null,
                  startInclusive: Boolean = false, end: Any = null,
                  endInclusive: Boolean = false,
                  includeDoc: Boolean = true,
                  allowQuery: Boolean = false): (String, Boolean, Boolean) = {
    val (url: String, pusheddown: Boolean, queryUsed: Boolean) =
      calculate(field, start, startInclusive, end, endInclusive, allowQuery)
    if (includeDoc && !queryUsed) {
      if (url.indexOf('?') > 0) {
        (url + "&include_docs=true", pusheddown, queryUsed)
      } else {
        (url + "?include_docs=true", pusheddown, queryUsed)
      }
    } else {
      (url, pusheddown, queryUsed)
    }
  }


  def getSubSetUrl(url: String, skip: Int, limit: Int, queryUsed: Boolean): String = {
    val suffix = {
      if (url.indexOf(JsonStoreConfigManager.ALL_DOCS_INDEX) > 0) {
        "include_docs=true&limit=" + limit + "&skip=" + skip
      } else if (viewPath != null) {
        "limit=" + limit + "&skip=" + skip
      } else if (queryUsed) {
        ""
      } else {
        "include_docs=true&limit=" + limit
      } // TODO Index query does not support subset query. Should disable Partitioned loading?
    }
    if (suffix.length == 0) {
      url
    } else if (url.indexOf('?') > 0) {
      url + "&" + suffix
    }
    else {
      url + "?" + suffix
    }
  }

  private def calculate(field: String, start: Any,
                        startInclusive: Boolean, end: Any, endInclusive: Boolean,
                        allowQuery: Boolean): (String, Boolean, Boolean) = {
    if (field != null && field.equals(pkField)) {
      var condition = ""
      if (start != null && end != null && start.equals(end)) {
        condition += "?key=%22" + URLEncoder.encode(start.toString, "UTF-8") + "%22"
      } else {
        if (start != null) {
          condition += "?startkey=%22" + URLEncoder.encode(
            start.toString, "UTF-8") + "%22"
        }
        if (end != null) {
          if (start != null) {
            condition += "&"
          } else {
            condition += "?"
          }
          condition += "endkey=%22" + URLEncoder.encode(end.toString, "UTF-8") + "%22"
        }
      }
      (dbUrl + "/" + defaultIndex + condition, true, false)
    } else if (indexPath != null) {
      //  push down to indexName
      val condition = calculateCondition(field, start, startInclusive,
        end, endInclusive)
      (dbUrl + "/" + indexPath + "?q=" + condition, true, false)
    } else if (viewPath != null) {
      (dbUrl + "/" + viewPath, false, false)
    } else if (allowQuery && useQuery) {
      (s"$dbUrl/_find", false, true)
    } else {
      (s"$dbUrl/$defaultIndex", false, false)
    }

  }

  def calculateCondition(field: String, min: Any, minInclusive: Boolean = false,
                         max: Any, maxInclusive: Boolean = false) : String = {
    if (field != null && (min != null || max!= null)) {
      var condition = field + ":"
      if (min!=null && max!=null && min.equals(max)) {
        condition += min
      } else {
        if (minInclusive) {
          condition+="["
        } else {
          condition +="{"
        }
        if (min!=null) {
          condition += min
        } else {
          condition+="*"
        }
        condition+=" TO "
        if (max !=null) {
          condition += max
        } else {
          condition += "*"
        }
        if (maxInclusive) {
          condition+="]"
        } else {
          condition +="}"
        }
      }
      URLEncoder.encode(condition, "UTF-8")
    } else {
      default_filter
    }
  }

  def getResultCount(result: String): Int = {
    val jsonResult: JsonObject = new JsonParser().parse(result).getAsJsonObject
    if (jsonResult.has("total_rows")) {
      jsonResult.get("total_rows").getAsInt
    } else if (jsonResult.has("pending")) {
      jsonResult.get("pending").getAsInt + 1
    } else {
      1
    }
  }

  def getRows(result: String, queryUsed: Boolean): Seq[JsonObject] = {
    val jsonResult: JsonObject = new JsonParser().parse(result).getAsJsonObject
    if (queryUsed) {
      if (jsonResult.has("docs")) {
        jsonResult.get("docs").getAsJsonArray.asScala
          .map(row => row.getAsJsonObject).toSeq
      } else {
        Seq()
      }
    } else {
      if (jsonResult.has("results")) {
        jsonResult.get("result").getAsJsonArray.asScala.map(row => row.getAsJsonObject
          .get("doc").getAsJsonObject).toSeq
      } else if (viewPath == null) {
        jsonResult.get("rows").getAsJsonArray.asScala.map(row => row.getAsJsonObject
          .get("doc").getAsJsonObject).toSeq
      } else {
        jsonResult.get("rows").getAsJsonArray.asScala.map(row => row.getAsJsonObject).toSeq
      }
    }
  }

  def getBulkRows(rows: List[String]): List[JsonObject] = {
    rows.map { x => JsonConverter.toJson(x).getAsJsonObject }
  }

  def getConflictErrStr: String = {
    """"error":"conflict""""
  }

  def getForbiddenErrStr: String = {
    """"error":"forbidden""""
  }
}
