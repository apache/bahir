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

package org.apache.bahir.datasource.webhdfs.util

import java.io._

import scala.collection.mutable.HashMap
import scala.sys.process._

/**
 * This Singleton is used to generate SSLTrustStore certification once. The assumption behind use of
 * this trust store is that this code would be executed on a machine which would be accessible from
 * all Spark executors which need to access the trust store
 */
object SSLTrustStore {

  var trustStoreFileMap: HashMap[String, File] = HashMap()

  /**
   * This function checks the availability of truststore for a particular site. If not it creates
   * a new one.
   */
  def getCertDetails(path: String): Tuple2[File, String] = {

  print("path in ssltrust store getCertDetails : " + path + "\n")

    val pathComp = path.split("/")

    val srvr = pathComp(2)

    val trustStorePword = "ts-password"

    val currDir = ("pwd" !!).trim
    val trustStore = currDir + "/" + srvr + "_trustStore.jks"

    val os = new java.io.ByteArrayOutputStream

    val tsExist = (s"ls $trustStore" #> os).!

    val f = if (tsExist == 0) {
      print("Using Existing Trust Store for SSL" + "\n")
      new java.io.File(trustStore)
    }
    else {
      val cert = srvr + "_cert"
      val cfl = new File(cert)

      (s"openssl s_client -showcerts -connect $srvr" #| "openssl x509 -outform PEM" #> cfl).!
      (s"keytool -import -trustcacerts -alias hadoop -file $cert -keystore $trustStore" +
        s" -storepass $trustStorePword -noprompt").!
      (s"rm -f $cert").!
      new java.io.File(trustStore)
    }

    new Tuple2(f, trustStorePword)
  }
}
