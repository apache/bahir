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

import com.google.gson._
import org.slf4j.LoggerFactory

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._

import org.apache.bahir.cloudant.CloudantConfig
import org.apache.bahir.cloudant.common.JsonUtil.JsonConverter

/**
 * JsonStoreRDDPartition defines each partition as a subset of a query result:
  * the limit rows returns and the skipped rows.
 */

private[cloudant] class JsonStoreRDDPartition(val url: String, val skip: Int, val limit: Int,
                                              val idx: Int, val config: CloudantConfig,
                                              val selector: JsonElement, val fields: JsonElement,
                                              val queryUsed: Boolean)
    extends Partition with Serializable{
  val index: Int = idx
}

/**
 *  The main purpose of JsonStoreRDD is to be able to create parallel read
 *  by partition for dataaccess getAll (by condition) scenarios
 *  defaultPartitions : how many partition intent,
 *  will be re-calculate based on the value based on total rows
 *  and minInPartition / maxInPartition )
 *  maxRowsInPartition: -1 means unlimited
 */
class JsonStoreRDD(sc: SparkContext, config: CloudantConfig)
    (implicit requiredcolumns: Array[String] = null,
              filters: Array[Filter] = null)
  extends RDD[String](sc, Nil) {

  private val logger = LoggerFactory.getLogger(getClass)

  private def getTotalPartition(totalRows: Int, queryUsed: Boolean): Int = {
    // Note: _changes API does not work for partition
    if (config.endpoint == JsonStoreConfigManager.CHANGES_INDEX) {
      1
    } else {
      if (totalRows == 0 || ! config.allowPartition(queryUsed) )  {
        1
      } else if (totalRows < config.partitions * config.minInPartition) {
        val total = totalRows / config.minInPartition
        if (total == 0 ) {
          total + 1
        } else {
          total
        }
      }
      else if (config.maxInPartition <=0) {
        config.partitions
      } else {
        val total = totalRows / config.maxInPartition
        if ( totalRows % config.maxInPartition != 0) {
          total + 1
        }
        else {
          total
        }
      }
    }
  }

  private def getLimitPerPartition(totalRows: Int, totalPartition: Int): Int = {
    val limit = totalRows/totalPartition
    if (totalRows % totalPartition != 0) {
      limit + 1
    } else {
      limit
    }
  }

  private def convertToMangoJson(f: Filter): (String, JsonElement) = {
    val gson = new Gson
    val parser = new JsonParser()
    val (op, value): (String, Any) = f match {
      case EqualTo(attr, v) => ("$eq", v)
      case GreaterThan(attr, v) => ("$gt", v)
      case LessThan(attr, v) => ("$lt", v)
      case GreaterThanOrEqual(attr, v) => ("$gte", v)
      case LessThanOrEqual(attr, v) => ("$lte", v)
      case _ => (null, null)
    }
    val convertedV: JsonElement = {
      // TODO Better handing of other types
      if (value != null) {
        value match {
          case s: String => parser.parse(s)
          case l: Long => parser.parse(l.toString)
          case d: Double => parser.parse(d.toString)
          case i: Int => parser.parse(i.toString)
          case b: Boolean => parser.parse(b.toString)
          case t: java.sql.Timestamp => parser.parse(t.toString)
          case a: Any => logger.debug(s"Ignore field:$name, cannot handle its datatype: $a"); null
        }
      } else null
    }
    (op, convertedV)
  }

  private def convertAttrToMangoJson(filters: Array[Filter]): Map[String, JsonElement] = {
    filters.map(af => convertToMangoJson(af))
            .filter(x => x._2 != null)
            .toMap
  }

  override def getPartitions: Array[Partition] = {

    logger.info("getPartitions:" + requiredcolumns + "," + filters)

    val filterInterpreter = new FilterInterpreter(filters)
    val origAttrToFilters = {
      if (filters == null || filters.length == 0) {
        null
      } else {
        filterInterpreter.getFiltersForPostProcess(null)
      }
    }

    val (selector, fields) : (JsonElement, JsonElement) = {
      if (!config.queryEnabled || origAttrToFilters == null) (null, null)
      else {
        val selectors: Map[String, Map[String, JsonElement]] =
          origAttrToFilters.transform( (name, attrFilters) => convertAttrToMangoJson(attrFilters))
        val filteredSelectors = selectors.filter((t) => t._2.nonEmpty)

        if (filteredSelectors.nonEmpty) {
          val queryColumns = {
              if (requiredcolumns == null || requiredcolumns.length == 0) {
                null
              } else {
                JsonConverter.toJson(requiredcolumns)
              }
          }
          (JsonConverter.toJson(filteredSelectors), queryColumns)
        } else (null, null)
      }
    }

    logger.info("calculated selector and fields:" + selector + "," + fields)

    var searchField: String = {
          if (origAttrToFilters ==null ) null
          else if (filterInterpreter.containsFiltersFor(config.pkField)) {
            config.pkField
          } else {
            filterInterpreter.firstField
          }
        }

    val (min, minInclusive, max, maxInclusive) = filterInterpreter.getInfo(searchField)
    val (url: String, pusheddown: Boolean, queryUsed: Boolean) = config.getRangeUrl(searchField,
            min, minInclusive, max, maxInclusive, includeDoc = false, selector != null)

    implicit val postData : String = {
      if (queryUsed) {
        val jsonSelector = new JsonObject
        jsonSelector.addProperty("selector", selector.toString)
        jsonSelector.addProperty("limit", 1)
        jsonSelector.toString
      } else {
        null
      }
    }
    val totalRows = new JsonStoreDataAccess(config).getTotalRows(url, queryUsed)
    val totalPartition = getTotalPartition(totalRows, queryUsed)
    val limitPerPartition = getLimitPerPartition(totalRows, totalPartition)

    logger.info(s"Partition config - total=$totalPartition, " +
        s"limit=$limitPerPartition for totalRows of $totalRows")

   logger.info(s"Partition query info - url=$url, queryUsed=$queryUsed")

   (0 until totalPartition).map(i => {
      val skip = i * limitPerPartition
      new JsonStoreRDDPartition(url, skip, limitPerPartition, i,
          config, selector, fields, queryUsed)
        .asInstanceOf[Partition]
    }).toArray
  }

  override def compute(splitIn: Partition, context: TaskContext):
      Iterator[String] = {
    val myPartition = splitIn.asInstanceOf[JsonStoreRDDPartition]
    implicit val postData : String = {
      val jsonObject = new JsonObject
      jsonObject.add("selector", myPartition.selector)
      jsonObject.addProperty("skip", myPartition.skip)
      if (myPartition.queryUsed && myPartition.fields != null) {
        jsonObject.add("fields", myPartition.fields)
        jsonObject.toString
      } else if (myPartition.queryUsed) {
        jsonObject.addProperty("limit", myPartition.limit)
        jsonObject.toString
      } else {
        null
      }
    }
    new JsonStoreDataAccess(myPartition.config).getIterator(myPartition.skip,
        myPartition.limit, myPartition.url)
  }
}
