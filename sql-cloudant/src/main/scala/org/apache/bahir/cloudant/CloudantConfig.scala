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

import java.net.URLEncoder

import play.api.libs.json.{JsArray, JsObject, Json, JsValue}

import org.apache.bahir.cloudant.common._

/*
* Only allow one field pushdown now
* as the filter today does not tell how to link the filters out And v.s. Or
*/

class CloudantConfig(val protocol: String, val host: String,
                     val dbName: String, val indexName: String, val viewName: String)
                    (implicit val username: String, val password: String,
                     val partitions: Int, val maxInPartition: Int, val minInPartition: Int,
                     val requestTimeout: Long, val bulkSize: Int, val schemaSampleSize: Int,
                     val createDBOnSave: Boolean, val endpoint: String,
                     val useQuery: Boolean = false, val queryLimit: Int)
  extends Serializable {

  lazy val dbUrl: String = {protocol + "://" + host + "/" + dbName}

  val pkField = "_id"
  val defaultIndex: String = endpoint
  val default_filter: String = "*:*"

  def getDbUrl: String = {
    dbUrl
  }

  def getSchemaSampleSize: Int = {
    schemaSampleSize
  }

  def getCreateDBonSave: Boolean = {
    createDBOnSave
  }

  def getLastNum(result: JsValue): JsValue = (result \ "last_seq").get

  /* Url containing limit for docs in a Cloudant database.
  * If a view is not defined, use the _all_docs endpoint.
  * @return url with one doc limit for retrieving total doc count
  */
  def getUrl(limit: Int, excludeDDoc: Boolean = false): String = {
    if (viewName == null) {
      val baseUrl = {
        if (excludeDDoc) {
          dbUrl + "/_all_docs?startkey=%22_design0/%22&include_docs=true"
        } else {
          dbUrl + "/_all_docs?include_docs=true"
        }
      }
      if (limit == JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
        baseUrl
      } else {
        baseUrl + "&limit=" + limit
      }
    } else {
      if (limit == JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
        dbUrl + "/" + viewName
      } else {
        dbUrl + "/" + viewName + "?limit=" + limit
      }
    }
  }

  /* Url containing limit to count total docs in a Cloudant database.
  *
  * @return url with one doc limit for retrieving total doc count
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
    useQuery && indexName == null && viewName == null
  }

  def allowPartition(queryUsed: Boolean): Boolean = {indexName==null && !queryUsed}

  def getRangeUrl(field: String = null, start: Any = null,
                  startInclusive: Boolean = false, end: Any = null,
                  endInclusive: Boolean = false,
                  includeDoc: Boolean = true,
                  allowQuery: Boolean = false): (String, Boolean, Boolean) = {
    val (url: String, pusheddown: Boolean, queryUsed: Boolean) =
      calculate(field, start, startInclusive, end, endInclusive, allowQuery)
    if (includeDoc && !queryUsed ) {
      if (url.indexOf('?') > 0) {
        (url + "&include_docs=true", pusheddown, queryUsed)
      } else {
        (url + "?include_docs=true", pusheddown, queryUsed)
      }
    } else {
      (url, pusheddown, queryUsed)
    }
  }

  /*
  * Url for paging using skip and limit options when loading docs with partitions.
  */
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
    } else if (indexName != null) {
      //  push down to indexName
      val condition = calculateCondition(field, start, startInclusive,
        end, endInclusive)
      (dbUrl + "/" + indexName + "?q=" + condition, true, false)
    } else if (viewName != null) {
      (dbUrl + "/" + viewName, false, false)
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

  def getBulkRows(rows: List[String]): String = {
    val docs = rows.map { x => Json.parse(x) }
    Json.stringify(Json.obj("docs" -> Json.toJson(docs.toSeq)))
  }

  def getConflictErrStr: String = {
    """"error":"conflict""""
  }

  def getForbiddenErrStr: String = {
    """"error":"forbidden""""
  }
}
