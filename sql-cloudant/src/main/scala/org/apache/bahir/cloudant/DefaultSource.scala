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

import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.bahir.cloudant.common.{JsonStoreDataAccess, JsonStoreRDD, _}
import org.apache.bahir.cloudant.internal.ChangesReceiver

case class CloudantReadWriteRelation (config: CloudantConfig,
                                      schema: StructType,
                                      dataFrame: DataFrame = null)
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan  with InsertableRelation {

   @transient lazy val dataAccess: JsonStoreDataAccess = {new JsonStoreDataAccess(config)}

    implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

    def buildScan(requiredColumns: Array[String],
                filters: Array[Filter]): RDD[Row] = {
      val colsLength = requiredColumns.length

      if (dataFrame != null) {
        if (colsLength == 0) {
          dataFrame.select().rdd
        } else if (colsLength == 1) {
          dataFrame.select(requiredColumns(0)).rdd
        } else {
          val colsExceptCol0 = for (i <- 1 until colsLength) yield requiredColumns(i)
          dataFrame.select(requiredColumns(0), colsExceptCol0: _*).rdd
        }
      } else {
        implicit val columns : Array[String] = requiredColumns
        implicit val origFilters : Array[Filter] = filters

        logger.info("buildScan:" + columns + "," + origFilters)
        val cloudantRDD = new JsonStoreRDD(sqlContext.sparkContext, config)
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
      if (config.getCreateDBonSave) {
        dataAccess.createDB()
      }
      if (data.count() == 0) {
        logger.warn("Database " + config.getDbname +
          ": nothing was saved because the number of records was 0!")
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

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String]): CloudantReadWriteRelation = {
      create(sqlContext, parameters, null)
    }

    private def create(sqlContext: SQLContext,
                       parameters: Map[String, String],
                       inSchema: StructType) = {

      val config: CloudantConfig = JsonStoreConfigManager.getConfig(sqlContext, parameters)

      var dataFrame: DataFrame = null

      val schema: StructType = {
        if (inSchema != null) {
          inSchema
        } else if (!config.isInstanceOf[CloudantChangesConfig]
          || config.viewName != null || config.indexName != null) {
          val df = if (config.getSchemaSampleSize ==
            JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT &&
            config.viewName == null
            && config.indexName == null) {
            val cloudantRDD = new JsonStoreRDD(sqlContext.sparkContext, config)
            dataFrame = sqlContext.read.json(cloudantRDD)
            dataFrame
          } else {
            val dataAccess = new JsonStoreDataAccess(config)
            val aRDD = sqlContext.sparkContext.parallelize(
                dataAccess.getMany(config.getSchemaSampleSize))
            sqlContext.read.json(aRDD)
          }
          df.schema
        } else {
          /* Create a streaming context to handle transforming docs in
          * larger databases into Spark datasets
          */
          val ssc = new StreamingContext(sqlContext.sparkContext, Seconds(10))

          val changesConfig = config.asInstanceOf[CloudantChangesConfig]
          val changes = ssc.receiverStream(
            new ChangesReceiver(changesConfig))
          changes.persist(changesConfig.getStorageLevelForStreaming)

          // Global RDD that's created from union of all RDDs
          var globalRDD = ssc.sparkContext.emptyRDD[String]

          logger.info("Loading data from Cloudant using "
            + changesConfig.getChangesReceiverUrl)

          // Collect and union each RDD to convert all RDDs to a DataFrame
          changes.foreachRDD((rdd: RDD[String]) => {
            if (!rdd.isEmpty()) {
              if (globalRDD != null) {
                // Union RDDs in foreach loop
                globalRDD = globalRDD.union(rdd)
              } else {
                globalRDD = rdd
              }
            } else {
              // Convert final global RDD[String] to DataFrame
              dataFrame = sqlContext.sparkSession.read.json(globalRDD)
              ssc.stop(stopSparkContext = false, stopGracefully = false)
            }
          })

          ssc.start
          // run streaming until all docs from continuous feed are received
          ssc.awaitTermination

          dataFrame.schema
        }
      }
      CloudantReadWriteRelation(config, schema, dataFrame)(sqlContext)
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
