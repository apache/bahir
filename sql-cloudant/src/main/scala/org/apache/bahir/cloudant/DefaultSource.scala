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

import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import org.apache.bahir.cloudant.common.{FilterInterpreter, JsonStoreDataAccess, JsonStoreRDD, _}

case class CloudantReadWriteRelation (config: CloudantConfig,
                                      schema: StructType,
                                      allDocsDF: DataFrame = null)
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan  with InsertableRelation {

   @transient lazy val dataAccess = {new JsonStoreDataAccess(config)}

    implicit lazy val logger = LoggerFactory.getLogger(getClass)

    def buildScan(requiredColumns: Array[String],
                filters: Array[Filter]): RDD[Row] = {
      val colsLength = requiredColumns.length

      if (allDocsDF != null) {
        if (colsLength == 0) {
          allDocsDF.select().rdd
        } else if (colsLength == 1) {
          allDocsDF.select(requiredColumns(0)).rdd
        } else {
          val colsExceptCol0 = for (i <- 1 until colsLength) yield requiredColumns(i)
          allDocsDF.select(requiredColumns(0), colsExceptCol0: _*).rdd
        }
      } else {
        val filterInterpreter = new FilterInterpreter(filters)
        var searchField: String = {
          if (filterInterpreter.containsFiltersFor(config.pkField)) {
            config.pkField
          } else {
            filterInterpreter.firstField
          }
        }

        val (min, minInclusive, max, maxInclusive) = filterInterpreter.getInfo(searchField)
        implicit val columns = requiredColumns
        val (url: String, pusheddown: Boolean) = config.getRangeUrl(searchField,
            min, minInclusive, max, maxInclusive, false)
        if (!pusheddown) searchField = null
        implicit val attrToFilters = filterInterpreter.getFiltersForPostProcess(searchField)

        val cloudantRDD = new JsonStoreRDD(sqlContext.sparkContext, config, url)
        val df = sqlContext.read.json(cloudantRDD)
        if (colsLength > 1) {
          val colsExceptCol0 = for (i <- 1 until colsLength) yield requiredColumns(i)
          df.select(requiredColumns(0), colsExceptCol0: _*).rdd
        } else {
          df.rdd
        }
      }
    }


  def insert(data: DataFrame, overwrite: Boolean): Unit = {
      if (config.getCreateDBonSave()) {
        dataAccess.createDB()
      }
      if (data.count() == 0) {
        logger.warn(("Database " + config.getDbname() +
          ": nothing was saved because the number of records was 0!"))
      } else {
        val result = data.toJSON.foreachPartition { x =>
          val list = x.toList // Has to pass as List, Iterator results in 0 data
          dataAccess.saveAll(list)
        }
      }
    }
}

class DefaultSource extends RelationProvider
  with CreatableRelationProvider
  with SchemaRelationProvider {

  val logger = LoggerFactory.getLogger(getClass)

  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String]): CloudantReadWriteRelation = {
      create(sqlContext, parameters, null)
    }

    private def create(sqlContext: SQLContext,
                       parameters: Map[String, String],
                       inSchema: StructType) = {

      val config: CloudantConfig = JsonStoreConfigManager.getConfig(sqlContext, parameters)

      var allDocsDF: DataFrame = null

      val schema: StructType = {
        if (inSchema != null) {
          inSchema
        } else {
          val df = if (config.getSchemaSampleSize() ==
            JsonStoreConfigManager.SCHEMA_FOR_ALL_DOCS_NUM &&
            config.viewName == null
            && config.indexName == null) {
            val filterInterpreter = new FilterInterpreter(null)
            var searchField = null
            val (min, minInclusive, max, maxInclusive) =
                filterInterpreter.getInfo(searchField)
            val (url: String, pusheddown: Boolean) = config.getRangeUrl(searchField,
                min, minInclusive, max, maxInclusive, false)
            val cloudantRDD = new JsonStoreRDD(sqlContext.sparkContext, config, url)
            allDocsDF = sqlContext.read.json(cloudantRDD)
            allDocsDF
          } else {
            val dataAccess = new JsonStoreDataAccess(config)
            val aRDD = sqlContext.sparkContext.parallelize(
                dataAccess.getMany(config.getSchemaSampleSize()))
            sqlContext.read.json(aRDD)
          }
          df.schema
        }
      }
      CloudantReadWriteRelation(config, schema, allDocsDF)(sqlContext)
    }

    def createRelation(sqlContext: SQLContext,
                       mode: SaveMode,
                       parameters: Map[String, String],
                       data: DataFrame): CloudantReadWriteRelation = {
      val relation = create(sqlContext, parameters, data.schema)
      relation.insert(data, mode==SaveMode.Overwrite)
      relation
    }

    def createRelation(sqlContext: SQLContext,
                       parameters: Map[String, String],
                       schema: StructType): CloudantReadWriteRelation = {
      create(sqlContext, parameters, schema)
    }

}
