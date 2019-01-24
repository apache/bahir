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

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.TimeLimits

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class SparkGCPCredentialsBuilderSuite
    extends SparkFunSuite with TimeLimits with BeforeAndAfter{
  private def builder = SparkGCPCredentials.builder

  private val jsonFilePath = sys.env.get(PubsubTestUtils.envVarNameForJsonKeyPath)
  private val p12FilePath = sys.env.get(PubsubTestUtils.envVarNameForP12KeyPath)
  private val emailAccount = sys.env.get(PubsubTestUtils.envVarNameForAccount)

  private def jsonAssumption() {
    assume(
      jsonFilePath.isDefined,
      s"as the environment variable ${PubsubTestUtils.envVarNameForJsonKeyPath} is not set.")
  }
  private def p12Assumption() {
    assume(
      p12FilePath.isDefined,
      s"as the environment variable ${PubsubTestUtils.envVarNameForP12KeyPath} is not set.")
    assume(
      emailAccount.isDefined,
      s"as the environment variable ${PubsubTestUtils.envVarNameForAccount} is not set.")
  }

  test("should build application default") {
    assert(builder.build() === ApplicationDefaultCredentials)
  }

  test("should build json service account") {
    jsonAssumption()

    assert(builder.jsonServiceAccount(jsonFilePath.get).build() != null)
  }

  test("should provide json credentials based on file") {
    jsonAssumption()

    val jsonCred = new JsonConfigCredentials(jsonFilePath.get)
    assert(jsonCred.provider.refreshToken, "Failed to retrieve new access token.")
  }

  test("should provide json credentials based on binary content") {
    jsonAssumption()

    val fileContent = Files.readAllBytes(Paths.get(jsonFilePath.get))
    val jsonCred = JsonConfigCredentials(fileContent)
    assert(jsonCred.provider.refreshToken, "Failed to retrieve new access token.")
  }

  test("should build p12 service account") {
    p12Assumption()

    assert(builder.p12ServiceAccount(p12FilePath.get, emailAccount.get).build() != null)
  }

  test("should provide p12 credentials based on file") {
    p12Assumption()

    val p12Cred = new EMailPrivateKeyCredentials(emailAccount.get, p12FilePath.get)
    assert(p12Cred.provider.refreshToken, "Failed to retrieve new access token.")
  }

  test("should provide p12 credentials based on binary content") {
    p12Assumption()

    val fileContent = Files.readAllBytes(Paths.get(p12FilePath.get))
    val p12Cred = EMailPrivateKeyCredentials(emailAccount.get, fileContent)
    assert(p12Cred.provider.refreshToken, "Failed to retrieve new access token.")
  }

  test("should build metadata service account") {
    val metadataCred = MetadataServiceCredentials()
    assertResult(metadataCred) {
      builder.metadataServiceAccount().build()
    }
  }

  test("SparkGCPCredentials classes should be serializable") {
    jsonAssumption()
    p12Assumption()

    val jsonCred = new JsonConfigCredentials(jsonFilePath.get)
    val p12Cred = new EMailPrivateKeyCredentials(emailAccount.get, p12FilePath.get)
    val metadataCred = MetadataServiceCredentials()

    val jsonCredDeserialized: JsonConfigCredentials = Utils.deserialize(Utils.serialize(jsonCred))
    assert(jsonCredDeserialized != null)

    val p12CredDeserialized: EMailPrivateKeyCredentials =
      Utils.deserialize(Utils.serialize(p12Cred))
    assert(p12CredDeserialized != null)

    assertResult(metadataCred) {
      Utils.deserialize(Utils.serialize(metadataCred))
    }

    assertResult(ApplicationDefaultCredentials) {
      Utils.deserialize(Utils.serialize(ApplicationDefaultCredentials))
    }
  }
}
