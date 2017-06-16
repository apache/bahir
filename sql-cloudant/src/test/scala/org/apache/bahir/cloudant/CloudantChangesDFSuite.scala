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

import play.libs.Json

import org.apache.spark.sql.SparkSession

class CloudantChangesDFSuite extends ClientSparkFunSuite {
  val endpoint = "_changes"

  before {
    spark = SparkSession.builder().config(conf)
      .config("cloudant.protocol", TestUtils.getProtocol)
      .config("cloudant.host", TestUtils.getHost)
      .config("cloudant.username", TestUtils.getUsername)
      .config("cloudant.password", TestUtils.getPassword)
      .config("cloudant.endpoint", endpoint)
      .config("spark.streaming.unpersist", "false")
      .getOrCreate()
  }

  after {
    spark.close()
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

  testIfEnabled("load data and verify deleted doc is not in results") {
    val db = client.database("n_flight", false)
    // Find then delete a doc to verify it's not included when loading data
    val doc = db.find("003bd483-9f98-4203-afdd-c539a4f38d21")
    val json = try {  Json.parse(doc) } finally { doc.close() }
    db.remove(json.get("_id").textValue(), json.get("_rev").textValue())

    val df = spark.read.format("org.apache.bahir.cloudant").load("n_flight")
    // all docs in database minus the design doc and _deleted=true doc
    assert(df.count() == 1966)

    assert(!df.columns.contains("_deleted"))
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

    // Saving dataframe to Cloudant db
    // to create a Cloudant db during save set the option createDBOnSave=true
    val df2 = df.filter(df("_id") >= "CAA")
      .select("_id", "airportName")
      .write.format("org.apache.bahir.cloudant")
      .option("createDBOnSave", "true")
      .save("airportcodemapping_df")

    val dfAirport = spark.read.format("org.apache.bahir.cloudant")
      .load("airportcodemapping_df")

    assert(dfAirport.count() == 13)

    deleteTestDb("airportcodemapping_df")
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

  testIfEnabled("load data and verify total count of selector, filter, and view option") {
    val df = spark.read.format("org.apache.bahir.cloudant")
      .option("selector", "{\"flightSegmentId\": {\"$eq\": \"AA2\"}}")
      .load("n_flight")
    val dfcount = df.count()
    assert(dfcount == 5)

    val df2 = spark.read.format("org.apache.bahir.cloudant")
      .load("n_flight")
    val df2count = df2.filter(df2("flightSegmentId") === "AA2")
      .select("flightSegmentId", "scheduledDepartureTime")
      .orderBy(df2("flightSegmentId")).count()
    assert(df2count == dfcount)

    val df3 = spark.read.format("org.apache.bahir.cloudant")
      .option("view", "_design/view/_view/AA2").load("n_flight")
    val df3count = df3.count()
    assert(dfcount == df3count)
    assert(df2count == df3count)
  }
}
