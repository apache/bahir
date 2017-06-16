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

  testIfEnabled("invalid api receiver option throws an error message") {
    spark = SparkSession.builder().config(conf)
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

  testIfEnabled("empty username option throws an error message") {
    spark = SparkSession.builder().config(conf)
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

  testIfEnabled("empty password option throws an error message") {
    spark = SparkSession.builder().config(conf)
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

  testIfEnabled("empty databaseName throws an error message") {
    spark = SparkSession.builder().config(conf)
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
}
