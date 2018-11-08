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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }

import org.apache.bahir.cloudant.CloudantReceiver

object CloudantStreamingSelector {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Cloudant Spark SQL External Datasource in Scala")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Create the context with a 10 seconds batch size
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val curTotalAmount = new AtomicLong(0)
    val curSalesCount = new AtomicLong(0)
    var batchAmount = 0L

    val changes = ssc.receiverStream(new CloudantReceiver(spark.sparkContext.getConf, Map(
      "cloudant.host" -> "examples.cloudant.com",
      "database" -> "sales",
      "selector" -> "{\"month\":\"May\", \"rep\":\"John\"}")))

    changes.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext

      println(s"========= $time =========") // scalastyle:ignore
      val changesDataFrame = spark.read.json(rdd.toDS())
      if (changesDataFrame.schema.nonEmpty) {
        changesDataFrame.select("*").show()
        batchAmount = changesDataFrame.groupBy().sum("amount").collect()(0).getLong(0)
        curSalesCount.getAndAdd(changesDataFrame.count())
        curTotalAmount.getAndAdd(batchAmount)
        println("Current sales count:" + curSalesCount)// scalastyle:ignore
        println("Current total amount:" + curTotalAmount)// scalastyle:ignore
      } else {
        ssc.stop()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
