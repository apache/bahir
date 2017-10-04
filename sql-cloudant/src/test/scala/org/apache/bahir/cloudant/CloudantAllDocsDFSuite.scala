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

import scala.util.Try

import org.apache.spark.sql.SparkSession

class CloudantAllDocsDFSuite extends ClientSparkFunSuite {
  val endpoint = "_all_docs"

  override def beforeAll() {
    super.beforeAll()
    spark = SparkSession.builder().config(conf)
      .config("cloudant.protocol", TestUtils.getProtocol)
      .config("cloudant.host", TestUtils.getHost)
      .config("cloudant.username", TestUtils.getUsername)
      .config("cloudant.password", TestUtils.getPassword)
      .config("cloudant.endpoint", endpoint)
      .getOrCreate()
  }

  testIfEnabled("load and save data from Cloudant database") {
    // Loading data from Cloudant db
    val df = spark.read.format("org.apache.bahir.cloudant").load("n_flight")
    // Caching df in memory to speed computations
    // and not to retrieve data from cloudant again
    df.cache()
    // all docs in database minus the design doc
    assert(df.count() == 1967)
  }

  testIfEnabled("load and count data from Cloudant search index") {
    val df = spark.read.format("org.apache.bahir.cloudant")
      .option("index", "_design/view/_search/n_flights").load("n_flight")
    val total = df.filter(df("flightSegmentId") >"AA9")
      .select("flightSegmentId", "scheduledDepartureTime")
      .orderBy(df("flightSegmentId")).count()
    assert(total == 50)
  }

  testIfEnabled("load data and count rows in filtered dataframe") {
    // Loading data from Cloudant db
    val df = spark.read.format("org.apache.bahir.cloudant")
      .load("n_airportcodemapping")
    val dfFilter = df.filter(df("_id") >= "CAA").select("_id", "airportName")
    assert(dfFilter.count() == 13)
  }

  // save data to Cloudant test
  testIfEnabled("save filtered dataframe to database") {
    val df = spark.read.format("org.apache.bahir.cloudant").load("n_flight")

    // Saving data frame with filter to Cloudant db
    val df2 = df.filter(df("flightSegmentId") === "AA106")
      .select("flightSegmentId", "economyClassBaseCost")

    assert(df2.count() == 5)

    df2.write.format("org.apache.bahir.cloudant").save("n_flight2")

    val dfFlight2 = spark.read.format("org.apache.bahir.cloudant").load("n_flight2")

    assert(dfFlight2.count() == 5)
  }

  // createDBOnSave option test
  testIfEnabled("save dataframe to database using createDBOnSave=true option") {
    val df = spark.read.format("org.apache.bahir.cloudant")
      .load("n_airportcodemapping")

    val saveDfToDb = "airportcodemapping_df"

    // If 'airportcodemapping_df' exists, delete it.
    Try {
      client.deleteDB(saveDfToDb)
    }

    // Saving dataframe to Cloudant db
    // to create a Cloudant db during save set the option createDBOnSave=true
    val df2 = df.filter(df("_id") >= "CAA")
      .select("_id", "airportName")
      .write.format("org.apache.bahir.cloudant")
      .option("createDBOnSave", "true")
      .save(saveDfToDb)

    val dfAirport = spark.read.format("org.apache.bahir.cloudant")
      .load(saveDfToDb)

    assert(dfAirport.count() == 13)
  }

  // view option tests
  testIfEnabled("load and count data from view") {
    val df = spark.read.format("org.apache.bahir.cloudant")
      .option("view", "_design/view/_view/AA0").load("n_flight")
    assert(df.count() == 5)
  }

  testIfEnabled("load data from view with MapReduce function") {
    val df = spark.read.format("org.apache.bahir.cloudant")
      .option("view", "_design/view/_view/AAreduce?reduce=true")
      .load("n_flight")
    assert(df.count() == 1)
  }
}
