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

package org.apache.spark.streaming.pubsub

import java.io.FileNotFoundException

import org.scalatest.concurrent.Timeouts

import org.apache.spark.util.Utils
import org.apache.spark.SparkFunSuite

class SparkGCPCredentialsBuilderSuite extends SparkFunSuite with Timeouts {
  private def builder = SparkGCPCredentials.builder

  private val jsonCreds = ServiceAccountCredentials(
    jsonFilePath = Option("json-key-path")
  )

  private val p12Creds = ServiceAccountCredentials(
    p12FilePath = Option("p12-key-path"),
    emailAccount = Option("email")
  )

  private val metadataCreds = ServiceAccountCredentials()

  test("should build application default") {
    assert(builder.build() === ApplicationDefaultCredentials)
  }

  test("should build json service account") {
    assertResult(jsonCreds) {
      builder.jsonServiceAccount(jsonCreds.jsonFilePath.get).build()
    }
  }

  test("should provide json creds") {
    val thrown = intercept[FileNotFoundException] {
      jsonCreds.provider
    }
    assert(thrown.getMessage === "json-key-path (No such file or directory)")
  }

  test("should build p12 service account") {
    assertResult(p12Creds) {
      builder.p12ServiceAccount(p12Creds.p12FilePath.get, p12Creds.emailAccount.get).build()
    }
  }

  test("should provide p12 creds") {
    val thrown = intercept[FileNotFoundException] {
      p12Creds.provider
    }
    assert(thrown.getMessage === "p12-key-path (No such file or directory)")
  }

  test("should build metadata service account") {
    assertResult(metadataCreds) {
      builder.metadataServiceAccount().build()
    }
  }

  test("SparkGCPCredentials classes should be serializable") {
    assertResult(jsonCreds) {
      Utils.deserialize[ServiceAccountCredentials](Utils.serialize(jsonCreds))
    }

    assertResult(p12Creds) {
      Utils.deserialize[ServiceAccountCredentials](Utils.serialize(p12Creds))
    }

    assertResult(metadataCreds) {
      Utils.deserialize[ServiceAccountCredentials](Utils.serialize(metadataCreds))
    }

    assertResult(ApplicationDefaultCredentials) {
      Utils.deserialize[ServiceAccountCredentials](Utils.serialize(ApplicationDefaultCredentials))
    }
  }

}
