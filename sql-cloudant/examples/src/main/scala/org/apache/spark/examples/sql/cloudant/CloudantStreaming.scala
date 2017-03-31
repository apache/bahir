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

package org.apache.spark.examples.sql.cloudant

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.streaming.scheduler.{ StreamingListener, StreamingListenerReceiverError}

import org.apache.bahir.cloudant.CloudantReceiver

object CloudantStreaming {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource in Scala")
    // Create the context with a 10 seconds batch size
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val changes = ssc.receiverStream(new CloudantReceiver(sparkConf, Map(
      "cloudant.host" -> "ACCOUNT.cloudant.com",
      "cloudant.username" -> "USERNAME",
      "cloudant.password" -> "PASSWORD",
      "database" -> "n_airportcodemapping")))

    changes.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SparkSession
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      println(s"========= $time =========")// scalastyle:ignore
      // Convert RDD[String] to DataFrame
      val changesDataFrame = spark.read.json(rdd)
      if (!changesDataFrame.schema.isEmpty) {
        changesDataFrame.printSchema()
        changesDataFrame.select("*").show()

        var hasDelRecord = false
        var hasAirportNameField = false
        for (field <- changesDataFrame.schema.fieldNames) {
          if ("_deleted".equals(field)) {
            hasDelRecord = true
          }
          if ("airportName".equals(field)) {
            hasAirportNameField = true
          }
        }
        if (hasDelRecord) {
          changesDataFrame.filter(changesDataFrame("_deleted")).select("*").show()
        }

        if (hasAirportNameField) {
          changesDataFrame.filter(changesDataFrame("airportName") >= "Paris").select("*").show()
          changesDataFrame.registerTempTable("airportcodemapping")
          val airportCountsDataFrame =
            spark.sql(
                s"""
                |select airportName, count(*) as total
                |from airportcodemapping
                |group by airportName
                """.stripMargin)
          airportCountsDataFrame.show()
        }
      }

    })
    ssc.start()
    // run streaming for 120 secs
    Thread.sleep(120000L)
    ssc.stop(true)
  }
}

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {
  @transient  private var instance: SparkSession = _
  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
