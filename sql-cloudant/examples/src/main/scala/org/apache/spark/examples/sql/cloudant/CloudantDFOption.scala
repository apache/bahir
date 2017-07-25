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

import org.apache.spark.sql.SparkSession

object CloudantDFOption{
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Cloudant Spark SQL Example with Dataframe using Option")
      .getOrCreate()

    val cloudantHost = "ACCOUNT.cloudant.com"
    val cloudantUser = "USERNAME"
    val cloudantPassword = "PASSWORD"

    // 1. Loading data from Cloudant db
    val df = spark.read.format("org.apache.bahir.cloudant")
      .option("cloudant.host", cloudantHost)
      .option("cloudant.username", cloudantUser)
      .option("cloudant.password", cloudantPassword)
      .load("n_airportcodemapping")

    df.cache()
    df.printSchema()
    df.filter(df("_id") >= "CAA").select("_id", "airportName").show()

    // 2. Saving dataframe to Cloudant db
    // To create a Cloudant db during save set the option createDBOnSave=true
    df.filter(df("_id") >= "CAA")
      .select("_id", "airportName")
      .write.format("org.apache.bahir.cloudant")
      .option("cloudant.host", cloudantHost)
      .option("cloudant.username", cloudantUser)
      .option("cloudant.password", cloudantPassword)
      .option("createDBOnSave", "true")
      .save("airportcodemapping_df")

    // 3. Loading data from Cloudant search index
    val df2 = spark.read.format("org.apache.bahir.cloudant")
      .option("index", "_design/view/_search/n_flights")
      .option("cloudant.host", cloudantHost)
      .option("cloudant.username", cloudantUser)
      .option("cloudant.password", cloudantPassword)
      .load("n_flight")
    val total2 = df2.filter(df2("flightSegmentId") >"AA9")
      .select("flightSegmentId", "scheduledDepartureTime")
      .orderBy(df2("flightSegmentId"))
      .count()
    println(s"Total $total2 flights from index")// scalastyle:ignore
  }
}
