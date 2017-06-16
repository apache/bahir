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

import org.apache.spark.sql.{SparkSession, SQLContext, SQLImplicits}

class CloudantSparkSQLSuite extends ClientSparkFunSuite {
  // import spark implicits
  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  import testImplicits._

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

  testIfEnabled("verify results from temp view of database n_airportcodemapping") {

    // create a temp table from Cloudant db and query it using sql syntax
    val sparkSql = spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW airportTable
         |USING org.apache.bahir.cloudant
         |OPTIONS ( database 'n_airportcodemapping')
        """.stripMargin)

    // create a dataframe
    val airportData = spark.sql(
      s"""
         |SELECT _id, airportName
         |FROM airportTable
         |WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id
        """.stripMargin)
    assert(airportData.count() == 4)

    // create filtered dataframe to compare with SQL temp. view
    val df2 = spark.read.format("org.apache.bahir.cloudant")
      .load("n_airportcodemapping")
    val df2count = df2.filter(df2("_id") >="CAA" && df2("_id") <="GAA")
      .select("_id", "airportName")
      .orderBy(df2("_id")).count()

    assert(df2count == airportData.count())
  }

  testIfEnabled("verify results from temp view of index in n_flight") {
    // create a temp table from Cloudant index  and query it using sql syntax
    val sparkSql = spark.sql(
      s"""
         |CREATE TEMPORARY VIEW flightTable
         |USING org.apache.bahir.cloudant
         |OPTIONS (database 'n_flight', index '_design/view/_search/n_flights')
        """.stripMargin)

    val flightData = spark.sql(
      s"""
         |SELECT flightSegmentId, scheduledDepartureTime
         |FROM flightTable
         |WHERE flightSegmentId >'AA9' AND flightSegmentId<'AA95'
        """.stripMargin)
    assert(flightData.count() == 25)

    // create filtered dataframe to compare with SQL temp. view
    val df2 = spark.read.format("org.apache.bahir.cloudant")
      .load("n_flight")
    val df2count = df2.filter(df2("flightSegmentId") > "AA9"
      && df2("flightSegmentId") < "AA95")
      .select("flightSegmentId", "scheduledDepartureTime")
      .orderBy(df2("_id")).count()

    assert(df2count == flightData.count())
  }
}
