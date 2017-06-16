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

import java.io.File

object TestUtils {
  // Set CouchDB/Cloudant host, username and password for local testing
  private val host = System.getenv("CLOUDANT_HOST")
  private val username = System.getenv("CLOUDANT_USER")
  private val password = System.getenv("CLOUDANT_PASSWORD")
  private val protocol = System.getenv("CLOUDANT_PROTOCOL")

  // List of test databases to create from JSON flat files
  val testDatabasesList: List[String] = List(
    "n_airportcodemapping",
    "n_booking",
    "n_customer",
    "n_customersession",
    "n_flight",
    "n_flight2",
    "n_flightsegment"
  )

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }

  // default value is https for cloudant.com accounts
  def getProtocol: String = {
    if (protocol != null && !protocol.isEmpty) {
      protocol
    } else {
      "https"
    }
  }

  def getHost: String = {
    if (host != null && !host.isEmpty) {
      host
    } else {
      getUsername + ".cloudant.com"
    }
  }

  def getUsername: String = {
    username
  }

  def getPassword: String = {
    password
  }

  lazy val shouldRunTests = {
    val isEnvSet = (username != null && !username.isEmpty) &&
      (password != null && !password.isEmpty)
    if (isEnvSet) {
      // scalastyle:off println
      // Print this so that they are easily visible on the console and not hidden in the log4j logs.
      println(
        s"""
           |Sql-cloudant tests that require Cloudant databases have been enabled by
           |the environment variables CLOUDANT_USER and CLOUDANT_PASSWORD.
        """.stripMargin)
      // scalastyle:on println
    }
    isEnvSet
  }
}
