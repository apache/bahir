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

import java.io.{File, FileReader}
import java.net.URL
import java.util

import com.cloudant.client.api.ClientBuilder
import com.cloudant.client.api.CloudantClient
import com.google.gson.{Gson, JsonArray, JsonObject}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession

import org.apache.bahir.cloudant.TestUtils.shouldRunTests

class ClientSparkFunSuite extends SparkFunSuite with BeforeAndAfter {
  private val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/sql-cloudant/")

  var client: CloudantClient = _
  val conf: SparkConf = new SparkConf().setMaster("local[4]")
  var deletedDoc = new JsonObject()
  var spark: SparkSession = _

  override def beforeAll() {
    runIfTestsEnabled("Prepare Cloudant test databases") {
      tempDir.mkdirs()
      tempDir.deleteOnExit()
      setupClient()
      createTestDbs()
    }
}

  override def afterAll() {
    TestUtils.deleteRecursively(tempDir)
    deleteTestDbs()
    teardownClient()
    spark.close()
  }

  def setupClient() {
    if (TestUtils.getHost.endsWith("cloudant.com")) {
      client = ClientBuilder
        .account(TestUtils.getUsername)
        .username(TestUtils.getUsername)
        .password(TestUtils.getPassword)
        .build
    } else {
      val host = TestUtils.getProtocol + "://" + TestUtils.getHost
      client = ClientBuilder.url(new URL(host))
        .username(TestUtils.getUsername)
        .password(TestUtils.getPassword)
        .build
    }
  }

  def teardownClient() {
    if (client != null) {
      client.shutdown()
    }
  }

  def createTestDbs() {
    // create each test database
    // insert docs and design docs from JSON flat files
    for (dbName: String <- TestUtils.testDatabasesList) {
      val db = client.database(dbName, true)
      val jsonFilePath = getClass.getResource("/json-files/" + dbName + ".json")
      if (jsonFilePath != null && new File(jsonFilePath.getFile).exists()) {
        val jsonFileArray = new Gson().fromJson(new FileReader(jsonFilePath.getFile),
          classOf[JsonArray])
        val listOfObjects = new util.ArrayList[JsonObject]
        if (jsonFileArray != null) {
          var i = 0
          while (i < jsonFileArray.size()) {
            listOfObjects.add(jsonFileArray.get(i).getAsJsonObject)
            i += 1
          }
        }

        val responses = db.bulk(listOfObjects)
        var i = 0
        while (i < responses.size()) {
          assert(responses.get(i).getStatusCode == 200 || responses.get(i).getStatusCode == 201)
          i += 1
        }
        if (dbName == "n_flight") {
          deletedDoc.addProperty("_id", responses.get(0).getId)
          deletedDoc.addProperty("_rev", responses.get(0).getRev)
        }

      }
    }
  }

  def deleteTestDbs() {
    for (db: String <- TestUtils.testDatabasesList) {
      deleteTestDb(db)
    }
  }

  def deleteTestDb(dbName: String) {
    client.deleteDB(dbName)
  }

  /** Run the test if environment variable is set or ignore the test */
  def testIfEnabled(testName: String)(testBody: => Unit) {
    if (shouldRunTests) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var CLOUDANT_USER and " +
        s"CLOUDANT_PASSWORD]")(testBody)
    }
  }


  /** Run the body of code only if tests are enabled */
  def runIfTestsEnabled(message: String)(body: => Unit): Unit = {
    if (shouldRunTests) {
      body
    } else {
      ignore(s"$message [enable by setting env var CLOUDANT_USER and " +
        s"CLOUDANT_PASSWORD]")(())
    }
  }
}
