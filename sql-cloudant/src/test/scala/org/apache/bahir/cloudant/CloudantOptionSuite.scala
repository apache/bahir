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

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession

import org.apache.bahir.cloudant.common.CloudantException

class CloudantOptionSuite extends ClientSparkFunSuite with BeforeAndAfter {

  after {
    spark.close()
  }

  testIf("invalid api receiver option throws an error message", () => TestUtils.shouldRunTest()) {
    spark = SparkSession.builder().config(conf)
      .config("cloudant.protocol", TestUtils.getProtocol)
      .config("cloudant.host", TestUtils.getHost)
      .config("cloudant.username", TestUtils.getUsername)
      .config("cloudant.password", TestUtils.getPassword)
      .config("cloudant.endpoint", "_invalid_endpoint")
      .getOrCreate()

    val thrown = intercept[CloudantException] {
      spark.read.format("org.apache.bahir.cloudant").load("db")
    }
    assert(thrown.getMessage === s"spark.cloudant.endpoint parameter " +
      s"is invalid. Please supply the valid option '_all_docs' or '_changes'.")
  }

  testIf("empty username option throws an error message", () => TestUtils.shouldRunTest()) {
    spark = SparkSession.builder().config(conf)
      .config("cloudant.protocol", TestUtils.getProtocol)
      .config("cloudant.host", TestUtils.getHost)
      .config("cloudant.username", "")
      .config("cloudant.password", TestUtils.getPassword)
      .getOrCreate()

    val thrown = intercept[CloudantException] {
      spark.read.format("org.apache.bahir.cloudant").load("db")
    }
    assert(thrown.getMessage === s"spark.cloudant.username parameter " +
      s"is empty. Please supply the required value.")
  }

  testIf("empty password option throws an error message", () => TestUtils.shouldRunTest()) {
    spark = SparkSession.builder().config(conf)
      .config("cloudant.protocol", TestUtils.getProtocol)
      .config("cloudant.host", TestUtils.getHost)
      .config("cloudant.username", TestUtils.getUsername)
      .config("cloudant.password", "")
      .getOrCreate()

    val thrown = intercept[CloudantException] {
      spark.read.format("org.apache.bahir.cloudant").load("db")
    }
    assert(thrown.getMessage === s"spark.cloudant.password parameter " +
      s"is empty. Please supply the required value.")
  }

  testIf("empty databaseName throws an error message", () => TestUtils.shouldRunTest()) {
    spark = SparkSession.builder().config(conf)
      .config("cloudant.protocol", TestUtils.getProtocol)
      .config("cloudant.host", TestUtils.getHost)
      .config("cloudant.username", TestUtils.getUsername)
      .config("cloudant.password", TestUtils.getPassword)
      .getOrCreate()

    val thrown = intercept[CloudantException] {
      spark.read.format("org.apache.bahir.cloudant").load()
    }
    assert(thrown.getMessage === s"Cloudant database name is empty. " +
      s"Please supply the required value.")
  }

  testIf("incorrect password throws an error message for changes receiver",
      () => TestUtils.shouldRunTest()) {
    spark = SparkSession.builder().config(conf)
      .config("cloudant.protocol", TestUtils.getProtocol)
      .config("cloudant.host", TestUtils.getHost)
      .config("cloudant.username", TestUtils.getUsername)
      .config("cloudant.password", TestUtils.getPassword.concat("a"))
      .config("cloudant.endpoint", "_changes")
      .getOrCreate()

    val thrown = intercept[CloudantException] {
      spark.read.format("org.apache.bahir.cloudant").load("n_flight")
    }
    assert(thrown.getMessage === "Error retrieving _changes feed data" +
      " from database 'n_flight' with response code 401: {\"error\":\"unauthorized\"," +
      "\"reason\":\"Name or password is incorrect.\"}")
  }

  testIf("string with valid value for cloudant.numberOfRetries option",
         () => TestUtils.shouldRunTest()) {
    spark = SparkSession.builder().config(conf)
      .config("cloudant.protocol", TestUtils.getProtocol)
      .config("cloudant.host", TestUtils.getHost)
      .config("cloudant.username", TestUtils.getUsername)
      .config("cloudant.password", TestUtils.getPassword)
      .config("cloudant.numberOfRetries", "5")
      .getOrCreate()

    val df = spark.read.format("org.apache.bahir.cloudant").load("n_booking")
    assert(df.count() === 2)
  }

  testIf("invalid value for cloudant.numberOfRetries option throws an error message",
      () => TestUtils.shouldRunTest()) {
    spark = SparkSession.builder().config(conf)
      .config("cloudant.protocol", TestUtils.getProtocol)
      .config("cloudant.host", TestUtils.getHost)
      .config("cloudant.username", TestUtils.getUsername)
      .config("cloudant.password", TestUtils.getPassword)
      .config("cloudant.numberOfRetries", "five")
      .getOrCreate()

    val thrown = intercept[CloudantException] {
      spark.read.format("org.apache.bahir.cloudant").load("db")
    }
    assert(thrown.getMessage === s"Option \'cloudant.numberOfRetries\' failed with exception " +
      s"java.lang.NumberFormatException: Illegal value for config key cloudant.numberOfRetries: " +
      s"""For input string: "five"""")
  }
}
