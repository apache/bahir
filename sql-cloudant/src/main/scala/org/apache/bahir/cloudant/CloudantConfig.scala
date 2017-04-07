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

import play.api.libs.json.JsArray
import play.api.libs.json.Json
import play.api.libs.json.JsValue

import org.apache.bahir.cloudant.common._

/*
* Only allow one field pushdown now
* as the filter today does not tell how to link the filters out And v.s. Or
*/

class CloudantConfig(val protocol: String, val host: String,
    val dbName: String, val indexName: String = null, val viewName: String = null)
    (implicit val username: String, val password: String,
    val partitions: Int, val maxInPartition: Int, val minInPartition: Int,
    val requestTimeout: Long, val bulkSize: Int, val schemaSampleSize: Int,
    val createDBOnSave: Boolean, val selector: String, val useQuery: Boolean = false,
    val queryLimit: Int)
    extends Serializable{

  private lazy val dbUrl = {protocol + "://" + host + "/" + dbName}

  val pkField = "_id"
  val defaultIndex = "_all_docs" // "_changes" does not work for partition
  val default_filter: String = "*:*"

  def getContinuousChangesUrl(): String = {
    var url = dbUrl + "/_changes?include_docs=true&feed=continuous&heartbeat=3000"
    if (selector != null) {
      url = url + "&filter=_selector"
    }
    url
  }

  def getSelector() : String = {
    selector
  }

  def getDbUrl(): String = {
    dbUrl
  }

  def getSchemaSampleSize(): Int = {
    schemaSampleSize
  }

  def getCreateDBonSave(): Boolean = {
    createDBOnSave
  }

  def getTotalUrl(url: String): String = {
    if (url.contains('?')) {
      url + "&limit=1"
    } else {
      url + "?limit=1"
    }
  }

  def getDbname(): String = {
    dbName
  }

  def queryEnabled(): Boolean = {useQuery && indexName==null && viewName==null}

  def allowPartition(queryUsed: Boolean): Boolean = {indexName==null && !queryUsed}

  def getAllDocsUrl(limit: Int, excludeDDoc: Boolean = false): String = {

    if (viewName == null) {
      val baseUrl = (
          if ( excludeDDoc) dbUrl + "/_all_docs?startkey=%22_design0/%22&include_docs=true"
          else dbUrl + "/_all_docs?include_docs=true"
          )
      if (limit == JsonStoreConfigManager.ALL_DOCS_LIMIT) {
        baseUrl
      } else {
        baseUrl + "&limit=" + limit
      }
    } else {
      if (limit == JsonStoreConfigManager.ALL_DOCS_LIMIT) {
        dbUrl + "/" + viewName
      } else {
        dbUrl + "/" + viewName + "?limit=" + limit
      }
    }
  }

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

  private def calculate(field: String, start: Any, startInclusive: Boolean,
      end: Any, endInclusive: Boolean, allowQuery: Boolean): (String, Boolean, Boolean) = {
    if (field != null && field.equals(pkField)) {
      var condition = ""
      if (start != null && end != null && start.equals(end)) {
        condition += "?key=%22" + URLEncoder.encode(start.toString(), "UTF-8") + "%22"
      } else {
        if (start != null) {
          condition += "?startkey=%22" + URLEncoder.encode(
              start.toString(), "UTF-8") + "%22"
        }
        if (end != null) {
          if (start != null) {
            condition += "&"
          } else {
            condition += "?"
          }
          condition += "endkey=%22" + URLEncoder.encode(end.toString(), "UTF-8") + "%22"
        }
      }
      (dbUrl + "/_all_docs" + condition, true, false)
    } else if (indexName!=null) {
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

  def getSubSetUrl (url: String, skip: Int, limit: Int, queryUsed: Boolean): String = {
    val suffix = {
      if (url.indexOf("_all_docs")>0) "include_docs=true&limit=" +
        limit + "&skip=" + skip
      else if (viewName != null) {
        "limit=" + limit + "&skip=" + skip
      } else if (queryUsed) {
        ""
      } else {
        "include_docs=true&limit=" + limit
      } // TODO Index query does not support subset query. Should disable Partitioned loading?
    }
    if (suffix.length==0) {
      url
    } else if (url.indexOf('?') > 0) {
      url + "&" + suffix
    }
    else {
      url + "?" + suffix
    }
  }

  def getTotalRows(result: JsValue): Int = {
    val tr = (result \ "total_rows").asOpt[Int]
    tr match {
      case None =>
        (result \ "pending").as[Int] + 1
      case Some(tr2) =>
        tr2
    }
  }

  def getRows(result: JsValue, queryUsed: Boolean): Seq[JsValue] = {
    if ( queryUsed ) {
      ((result \ "docs").as[JsArray]).value.map(row => row)
    } else if ( viewName == null) {
      ((result \ "rows").as[JsArray]).value.map(row => (row \ "doc").get)
    } else {
      ((result \ "rows").as[JsArray]).value.map(row => row)
    }
  }

  def getBulkPostUrl(): String = {
    dbUrl + "/_bulk_docs"
  }

  def getBulkRows(rows: List[String]): String = {
    val docs = rows.map { x => Json.parse(x) }
    Json.stringify(Json.obj("docs" -> Json.toJson(docs.toSeq)))
  }

  def getConflictErrStr(): String = {
    """"error":"conflict""""
  }

  def getForbiddenErrStr(): String = {
    """"error":"forbidden""""
  }
}
