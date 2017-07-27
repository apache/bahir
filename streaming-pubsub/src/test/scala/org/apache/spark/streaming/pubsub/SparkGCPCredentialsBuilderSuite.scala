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

import java.nio.file.{Files, Paths}

import org.scalatest.concurrent.Timeouts
import org.scalatest.BeforeAndAfter

import org.apache.spark.util.Utils
import org.apache.spark.SparkFunSuite

class SparkGCPCredentialsBuilderSuite
    extends SparkFunSuite with Timeouts with BeforeAndAfter{
  private def builder = SparkGCPCredentials.builder

  private val jsonFilePath = sys.env.get(PubsubTestUtils.envVarNameForJsonKeyPath)
  private val p12FilePath = sys.env.get(PubsubTestUtils.envVarNameForP12KeyPath)
  private val emailAccount = sys.env.get(PubsubTestUtils.envVarNameForAccount)

  private def jsonAssumption {
    assume(
      !jsonFilePath.isEmpty,
      s"as the environment variable ${PubsubTestUtils.envVarNameForJsonKeyPath} is not set.")
  }
  private def p12Assumption {
    assume(
      !p12FilePath.isEmpty,
      s"as the environment variable ${PubsubTestUtils.envVarNameForP12KeyPath} is not set.")
    assume(
      !emailAccount.isEmpty,
      s"as the environment variable ${PubsubTestUtils.envVarNameForAccount} is not set.")
  }

  test("should build application default") {
    assert(builder.build() === ApplicationDefaultCredentials)
  }

  test("should build json service account") {
    jsonAssumption

    val jsonCreds = ServiceAccountCredentials(jsonFilePath = jsonFilePath)
    assertResult(jsonCreds) {
      builder.jsonServiceAccount(jsonFilePath.get).build()
    }
  }

  test("should provide json creds") {
    jsonAssumption

    val jsonCreds = ServiceAccountCredentials(jsonFilePath = jsonFilePath)
    val credential = jsonCreds.provider
    assert(credential.refreshToken, "Failed to retrive a new access token.")
  }

  test("should build p12 service account") {
    p12Assumption

    val p12Creds = ServiceAccountCredentials(
      p12FilePath = p12FilePath, emailAccount = emailAccount)
    assertResult(p12Creds) {
      builder.p12ServiceAccount(p12FilePath.get, emailAccount.get).build()
    }
  }

  test("should provide p12 creds") {
    p12Assumption

    val p12Creds = ServiceAccountCredentials(
      p12FilePath = p12FilePath, emailAccount = emailAccount)
    val credential = p12Creds.provider
    assert(credential.refreshToken, "Failed to retrive a new access token.")
  }

  test("should build metadata service account") {
    val metadataCreds = ServiceAccountCredentials()
    assertResult(metadataCreds) {
      builder.metadataServiceAccount().build()
    }
  }

  test("SparkGCPCredentials classes should be serializable") {
    jsonAssumption
    p12Assumption

    val jsonCreds = ServiceAccountCredentials(jsonFilePath = jsonFilePath)
    val p12Creds = ServiceAccountCredentials(
      p12FilePath = p12FilePath, emailAccount = emailAccount)
    val metadataCreds = ServiceAccountCredentials()
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
