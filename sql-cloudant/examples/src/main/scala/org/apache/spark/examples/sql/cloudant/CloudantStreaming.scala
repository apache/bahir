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
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import org.apache.bahir.cloudant.CloudantReceiver

object CloudantStreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Cloudant Spark SQL External Datasource in Scala")
      .master("local[*]")
      .getOrCreate()

    // Create the context with a 10 seconds batch size
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import spark.implicits._

    val changes = ssc.receiverStream(new CloudantReceiver(spark.sparkContext.getConf, Map(
      "cloudant.host" -> "examples.cloudant.com",
      "database" -> "sales")))

    changes.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SparkSession


      println(s"========= $time =========")// scalastyle:ignore
      // Convert RDD[String] to Dataset[String]
      val changesDataFrame = spark.read.json(rdd.toDS())
      if (changesDataFrame.schema.nonEmpty) {
        changesDataFrame.printSchema()

        var hasDelRecord = false
        var hasMonth = false
        for (field <- changesDataFrame.schema.fieldNames) {
          if ("_deleted".equals(field)) {
            hasDelRecord = true
          }
          if ("month".equals(field)) {
            hasMonth = true
          }
        }
        if (hasDelRecord) {
          changesDataFrame.filter(changesDataFrame("_deleted")).select("*").show()
        }

        if (hasMonth) {
          changesDataFrame.filter(changesDataFrame("month") === "May").select("*").show(5)
          changesDataFrame.createOrReplaceTempView("sales")
          val salesInMayCountsDataFrame =
            spark.sql(
              s"""
                 |select rep, amount
                 |from sales
                 |where month = "May"
                """.stripMargin)
          salesInMayCountsDataFrame.show(5)
        }
      }

    })
    ssc.start()
    // run streaming for 60 secs
    Thread.sleep(60000L)
    ssc.stop(true)
  }
}
