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

object CloudantApp {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Cloudant Spark SQL Example")
      .config("cloudant.host", "ACCOUNT.cloudant.com")
      .config("cloudant.username", "USERNAME")
      .config("cloudant.password", "PASSWORD")
      .getOrCreate()

    // For implicit conversions of Dataframe to RDDs
    import spark.implicits._

    // create a temp table from Cloudant db and query it using sql syntax
    spark.sql(
        s"""
        |CREATE TEMPORARY VIEW airportTable
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
    airportData.printSchema()
    println(s"Total # of rows in airportData: " + airportData.count()) // scalastyle:ignore
    // convert dataframe to array of Rows, and process each row
    airportData.map(t => "code: " + t(0) + ",name:" + t(1)).collect().foreach(println) // scalastyle:ignore

    // create a temp table from Cloudant index  and query it using sql syntax
    spark.sql(
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
    flightData.printSchema()
    flightData.map(t => "flightSegmentId: " + t(0) + ", scheduledDepartureTime: " + t(1))
      .collect().foreach(println) // scalastyle:ignore
  }
}
