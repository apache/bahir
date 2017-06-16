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

import org.slf4j.LoggerFactory
import play.api.libs.json.{JsObject, JsString, JsValue}

import org.apache.spark.sql.sources._


/**
 * Only handles the following filter condition
 * 1. EqualTo,GreaterThan,LessThan,GreaterThanOrEqual,LessThanOrEqual,In
 * 2. recursive AND of (filters in 1 and AND). Issue: Spark 1.3.0 does not return
 *    AND filter instead returned 2 filters
 */
class FilterInterpreter(origFilters: Array[Filter]) {

  private val logger = LoggerFactory.getLogger(getClass)

  lazy val firstField: String = {
    if (origFilters.length > 0) getFilterAttribute(origFilters(0))
    else null
  }

  private lazy val filtersByAttr = {
    origFilters
      .filter(f => getFilterAttribute(f) != null)
      .map(f => (getFilterAttribute(f), f))
      .groupBy(attrFilter => attrFilter._1)
      .mapValues(a => a.map(p => p._2))
  }

  private def getFilterAttribute(f: Filter): String = {
    val result = f match {
      case EqualTo(attr, v) => attr
      case GreaterThan(attr, v) => attr
      case LessThan(attr, v) => attr
      case GreaterThanOrEqual(attr, v) => attr
      case LessThanOrEqual(attr, v) => attr
      case In(attr, v) => attr
      case IsNotNull(attr) => attr
      case IsNull(attr) => attr
      case _ => null
    }
    result
  }

  def containsFiltersFor(key: String): Boolean = {
    filtersByAttr.contains(key)
  }

  private lazy val analyzedFilters = {
    filtersByAttr.map(m => m._1 -> analyze(m._2))
  }

  private def analyze(filters: Array[Filter]): (Any, Boolean, Any, Boolean, Array[Filter]) = {

    var min: Any = null
    var minInclusive: Boolean = false
    var max: Any = null
    var maxInclusive: Boolean = false
    var others: Array[Filter] = Array[Filter]()

    def evaluate(filter: Filter) {
      filter match {
        case GreaterThanOrEqual(attr, v) => min = v; minInclusive = true
        case LessThanOrEqual(attr, v) => max = v; maxInclusive = true
        case EqualTo(attr, v) => min = v; max = v
        case GreaterThan(attr, v) => min = v
        case LessThan(attr, v) => max = v
        case _ => others = others :+ filter
      }
    }

    filters.map(f => evaluate(f))

    logger.info(s"Calculated range info: min=$min," +
      s" minInclusive=$minInclusive," +
      s"max=$max," +
      s"maxInclusive=$maxInclusive," +
      s"others=$others")
    (min, minInclusive, max, maxInclusive, others)
  }

  def getInfo(field: String): (Any, Boolean, Any, Boolean) = {
    if (field == null) (null, false, null, false)
    else {
      val data = analyzedFilters.getOrElse(field, (null, false, null, false, null))
      (data._1, data._2, data._3, data._4)
    }
  }

  def getFiltersForPostProcess(pushdownField: String): Map[String, Array[Filter]] = {
    filtersByAttr.map(f => {
      if (f._1.equals(pushdownField)) f._1 -> analyzedFilters.get(pushdownField).get._5
      else f._1 -> f._2
    })
  }
}

/**
 *
 */
class FilterUtil(filters: Map[String, Array[Filter]]) {
  private val logger = LoggerFactory.getLogger(getClass)
  def apply(implicit r: JsValue = null): Boolean = {
    if (r == null) return true
    val satisfied = filters.forall({
      case (attr, filters) =>
        val field = JsonUtil.getField(r, attr).getOrElse(null)
        if (field == null) {
          logger.debug(s"field $attr not exisit:$r")
          false
        } else {
          true
        }
    })
    satisfied
  }
}


object FilterDDocs {
  def filter(row: JsValue): Boolean = {
    if (row == null) return true
    val id : String = if (row.as[JsObject].keys.contains("_id")) {
      JsonUtil.getField(row, "_id").orNull.as[JsString].value
    } else if (row.as[JsObject].keys.contains("id")) {
      JsonUtil.getField(row, "id").orNull.as[JsString].value
    } else {
      null
    }
    if (id != null && id.startsWith("_design")) {
      false
    } else {
      true
    }
  }
}
