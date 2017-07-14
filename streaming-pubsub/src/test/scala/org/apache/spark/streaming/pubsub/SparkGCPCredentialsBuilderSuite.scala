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

import java.io.{File, IOException, FileOutputStream, InputStream, OutputStream}
import java.security.SignatureException

import org.scalatest.concurrent.Timeouts
import org.scalatest.{BeforeAndAfter, FunSuite};

import org.apache.spark.util.Utils
import org.apache.spark.SparkFunSuite

class SparkGCPCredentialsBuilderSuite
    extends FunSuite with Timeouts with BeforeAndAfter{
  private def builder = SparkGCPCredentials.builder
  private val jsonResourcePath = "/org/apache/spark/streaming/pubusb/key-file.json"
  private val p12ResourcePath = "/org/apache/spark/streaming/pubusb/key-file.p12"

  private var jsonFilePath: Option[String] = null
  private var p12FilePath: Option[String] = null
  private val emailAccount = Option(
    "pubsub-subscriber@apache-bahir-streaming-pubsub.iam.gserviceaccount.com")

  before {
    def streamPipe(input: InputStream, output: OutputStream) {
      val BUFFER_SIZE: Int = 1024;
      val buffer: Array[Byte] = new Array[Byte](_length = BUFFER_SIZE);
      var n: Int = input.read(buffer);
      while (n > 0) {
        output.write(buffer, 0, n);
        n = input.read(buffer);
      }
    }

    val jsonIn = getClass.getResourceAsStream(jsonResourcePath)
    val jsonFile = File.createTempFile("key-file", "json")
    jsonFile.deleteOnExit
    val jsonOut = new FileOutputStream(jsonFile)
    streamPipe(jsonIn, jsonOut)
    jsonIn.close
    jsonOut.close
    jsonFilePath = Some(jsonFile.getPath)

    val p12In = getClass.getResourceAsStream(p12ResourcePath)
    val p12File = File.createTempFile("key-file", "p12")
    p12File.deleteOnExit
    val p12Out = new FileOutputStream(p12File)
    streamPipe(p12In, p12Out)
    p12In.close
    p12Out.close
    p12FilePath = Some(p12File.getPath)
  }

  test("should build application default") {
    assert(builder.build() === ApplicationDefaultCredentials)
  }

  test("should build json service account") {
    val jsonCreds = ServiceAccountCredentials(jsonFilePath = jsonFilePath)
    assertResult(jsonCreds) {
      builder.jsonServiceAccount(jsonFilePath.get).build()
    }
  }

  test("should provide json creds") {
    val jsonCreds = ServiceAccountCredentials(jsonFilePath = jsonFilePath)
    val credential = jsonCreds.provider
    intercept[IOException] {
      credential.refreshToken
    }
  }

  test("should build p12 service account") {
    val p12Creds = ServiceAccountCredentials(
      p12FilePath = p12FilePath, emailAccount = emailAccount)
    assertResult(p12Creds) {
      builder.p12ServiceAccount(p12FilePath.get, emailAccount.get).build()
    }
  }

  test("should provide p12 creds") {
    val p12Creds = ServiceAccountCredentials(
      p12FilePath = p12FilePath, emailAccount = emailAccount)
    val credential = p12Creds.provider
    intercept[IOException] {
      credential.refreshToken
    }
  }

  test("should build metadata service account") {
    val metadataCreds = ServiceAccountCredentials()
    assertResult(metadataCreds) {
      builder.metadataServiceAccount().build()
    }
  }

  test("SparkGCPCredentials classes should be serializable") {
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
