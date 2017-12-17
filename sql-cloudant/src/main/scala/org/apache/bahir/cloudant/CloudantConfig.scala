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
import scala.collection.mutable
import scala.reflect.io.File

import com.cloudant.client.api.{ClientBuilder, CloudantClient, Database}
import com.cloudant.client.api.model.SearchResult
import com.cloudant.client.api.views._
import com.cloudant.http.{Http, HttpConnection}
import com.cloudant.http.interceptors.Replay429Interceptor
import com.google.gson.{Gson, JsonObject}
import play.api.libs.json.{JsArray, JsObject, JsValue}

import org.apache.bahir.cloudant.common._

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
                     val useQuery: Boolean = false, val queryLimit: Int)
  extends Serializable {

  @transient private lazy val client: CloudantClient = ClientBuilder
    .url(getClientUrl)
    .username(username)
    .password(password)
    .interceptors(Replay429Interceptor.WITH_DEFAULTS)
    .build
  @transient private lazy val database: Database = client.database(dbName, false)
  lazy val dbUrl: String = {protocol + "://" + host + "/" + dbName}
  lazy val designDoc: String = {
    if (viewPath != null && viewPath.nonEmpty) {
      viewPath.split("/")(1)
    } else {
    null
    }
  }
  lazy val searchName: String = {
    // verify that the index path matches '_design/ddoc/_search/searchname'
    if (indexPath != null && indexPath.nonEmpty && indexPath.matches("\\w+\\/\\w+\\/\\w+\\/\\w+")) {
      val splitPath = indexPath.split(File.separator)
      // return 'design-doc/search-name'
      splitPath(1) + File.separator + splitPath(3)
    } else {
      null
    }
  }
  lazy val viewName: String = {
    // verify that the index path matches '_design/ddoc/_view/searchname'
    if (viewPath != null && viewPath.nonEmpty) {
      val splitViewPath = viewPath.split(File.separator)
      if(splitViewPath(3).contains("?")) {
        splitViewPath(3).substring(0, splitViewPath(3).indexOf("?"))
      } else {
        splitViewPath(3)
      }
    } else {
      null
    }
  }

  val pkField = "_id"
  val defaultIndex: String = endpoint
  val default_filter: String = "*:*"

  def buildAllDocsRequest(limit: Int, includeDocs: Boolean = true): AllDocsRequestBuilder = {
    var allDocsReq = database.getAllDocsRequestBuilder.includeDocs(includeDocs)
    if (limit != JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
      allDocsReq = allDocsReq.limit(limit)
    }
    allDocsReq
  }

  def buildViewRequest(limit: Int, includeDocs: Boolean = true):
  UnpaginatedRequestBuilder[String, String] = {
    val viewReq = database.getViewRequestBuilder(designDoc, viewName)
      .newRequest(Key.Type.STRING, classOf[String])
      .includeDocs(includeDocs)
    if (limit != JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
      viewReq.limit(limit)
    }
    viewReq
  }

  def buildSearchRequest(limit: Int): SearchResult[JsonObject] = {
    val searchReq = database.search(searchName)
    if (limit != JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
      searchReq.limit(limit)
    }
    searchReq.querySearchResult(default_filter, classOf[JsonObject])
  }

  def executeRequest(stringUrl: String, postData: String = null): HttpConnection = {
    val url = new URL(stringUrl)
    if(postData != null) {
      val conn = Http.POST(url, "application/json")
      conn.setRequestBody(postData)
      conn.requestProperties.put("Accept", "application/json")
      conn.requestProperties.put("User-Agent", "spark-cloudant")
      client.executeRequest(conn)
    } else {
      val conn = Http.GET(url)
      conn.requestProperties.put("Accept", "application/json")
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

  def getLastNum(result: JsValue): JsValue = (result \ "last_seq").get

  /* Url containing limit for docs in a Cloudant database.
  * If a view is not defined, use the _all_docs endpoint.
  * @return url with one doc limit for retrieving total doc count
  */
  def getUrl(limit: Int, excludeDDoc: Boolean = false): String = {
    if (viewPath == null) {
      val baseUrl = {
        // Note: Handle removal of ddocs during load
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

  def getTotalDocCount: Int = {
    val limit = 1
    if (viewPath != null) {
      // "limit=" + limit + "&skip=" + skip
      buildViewRequest(limit, includeDocs = false).build().getResponse.getTotalRowCount.toInt
    } else {
      // /_all_docs?limit=1
      // Note: java-cloudant's AllDocsRequest doesn't have a getTotalRowCount method
      // buildAllDocsRequest(1, includeDocs = false).build().getResponse.getTotalRowCount.toInt
      val gson = new Gson()
      val response = client.executeRequest(Http.GET(
        new URL(database.getDBUri + File.separator + endpoint + "?limit=" + limit)))
      getResultTotalRows(gson.fromJson(response.responseAsString, classOf[JsonObject]))
    }
  }

  def getDocs(limit: Int): List[JsonObject] = {
    if (viewPath != null) {
      // "limit=" + limit + "&skip=" + skip
      buildViewRequest(limit).build().getResponse.getDocsAs(classOf[JsonObject]).asScala.toList
    } else if (indexPath != null) {
      var searchDocs = mutable.ListBuffer[JsonObject]()
      for (result: SearchResult[JsonObject]#SearchResultRow <-
           buildSearchRequest(limit).getRows.asScala) {
        searchDocs += result.getDoc
      }
      searchDocs.toList
    } else {
      // /_all_docs?limit=1
      // val response = client.executeRequest(Http.GET(
      //   new URL(database.getDBUri + File.separator + endpoint + "?limit=1")))
      buildAllDocsRequest(limit).build().getResponse.getDocsAs(classOf[JsonObject]).asScala.toList
    }
  }

  def getDbname: String = {
    dbName
  }

  def queryEnabled: Boolean = {
    useQuery && indexPath == null && viewName == null
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
      } else if (viewName != null) {
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

  def getTotalRows(result: JsValue): Int = {
    val resultKeys = result.as[JsObject].keys
    if(resultKeys.contains("total_rows")) {
      (result \ "total_rows").as[Int]
    } else if (resultKeys.contains("pending")) {
      (result \ "pending").as[Int] + 1
    } else {
      1
    }
  }

  def getResultTotalRows(result: JsonObject): Int = {
    if (result.has("total_rows")) {
      result.get("total_rows").getAsInt
    } else if (result.has("pending")) {
      result.get("pending").getAsInt + 1
    } else {
      1
    }
  }

  def getRows(result: JsValue, queryUsed: Boolean): Seq[JsValue] = {
    if ( queryUsed ) {
      (result \ "docs").as[JsArray].value.map(row => row)
    } else {
      val containsResultsKey: Boolean = result.as[JsObject].keys.contains("results")
      if (containsResultsKey) {
        (result \ "results").as[JsArray].value.map(row => (row \ "doc").get)
      } else if (viewName == null) {
        (result \ "rows").as[JsArray].value.map(row => (row \ "doc").get)
      } else {
        (result \ "rows").as[JsArray].value.map(row => row)
      }
    }
  }

  def getBulkPostUrl: String = {
    dbUrl + "/_bulk_docs"
  }

  def getBulkRows(rows: List[String]): List[JsonObject] = {
    val gson = new Gson()
    rows.map { x => gson.fromJson(x, classOf[JsonObject]) }
  }

  def getConflictErrStr: String = {
    """"error":"conflict""""
  }

  def getForbiddenErrStr: String = {
    """"error":"forbidden""""
  }
}
