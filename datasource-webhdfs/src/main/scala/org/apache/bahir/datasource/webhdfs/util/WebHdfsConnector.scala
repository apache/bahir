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
import java.security._
import javax.net.ssl.{SSLContext, SSLSocketFactory, TrustManagerFactory}

import scala.annotation.switch

import scalaj.http.{Http, HttpOptions}

/**
 * This object contains all utility functions for reading/writing data from/to remote webhdfs
 * server. The abstraction maintained in this layer is at the level of RDD
 */
object  WebHdfsConnector {

  /*
   * This function returns a Tuple for credential store which contains flag for validating
   * Certificate, the Certificate File object and Certificate File Object password
   */
  def createTrustStoreCredForExecutors(cred: String, path: String): Tuple3[String, File, String] = {
    val trustStoreMap = {
      if (cred != "") {
        if (cred == "N") {
          new Tuple3("N", null, "")
        } else if (cred == "Y") {
          val tsd = SSLTrustStore.getCertDetails(path)
          new Tuple3("Y", tsd._1, tsd._2)
        } else {
          throw new Exception("Invalid Certificate Validation Option")
        }
      } else {
        new Tuple3("", null, "")
      }
    }
    trustStoreMap
  }


  /**
   * This function returns a SSLSocketFactory which needs to be used in HTTP connection library in
   * case Certificate to be validated
   */
  def biocSslSocketFactory(fl: File, pswrd: String): SSLSocketFactory = {
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
    val ks = KeyStore.getInstance("JKS")
    val fis = new java.io.FileInputStream(fl)
    ks.load(fis, pswrd.toCharArray());
    tmf.init(ks);

    val sslc = SSLContext.getInstance("SSL")

    sslc.init(null, tmf.getTrustManagers(), null)

    sslc.getSocketFactory()
  }

  //  /**
  //    * This function returns the list of files in a folder with file details as RDD
  //   */
  //  def listFromWebHdfs(sc: SparkContext, path: String,  trustStoreCred: String, userCred: String,
  // connProp: String): RDD[String]  = {
  //
  //
  //
  //    val conns = connProp.split(":")
  //
  //    val conn = Array(conns(0).toInt, conns(1).toInt)
  //
  //    val usrCred = userCred.split(":")
  //
  //    val trustCred = createTrustStoreCredForExecutors(trustStoreCred, path)
  //
  //    val fileDetails = getFilesDetails(path, trustCred, usrCred, conn)
  //
  //    def g(v:Tuple4[String, Long, Long, Int]) = v._1.split("/").last + "," +
  // v._2.toString + "," +  v._3.toString + "," +  v._4.toString + "\n"
  //
  //    val fds = fileDetails.map(x => g(x))
  //
  //    val flRdd = sc.parallelize(fds)
  //
  //    flRdd
  //
  //  }
  //
  //  /**
  //   * This function writes data back to hdfs using WebHDFS using multiple parallel connections.
  // Right now file overwrite is not supported
  //   */
  //  def writeToWebHdfs(dataToWrite: RDD[String] ,path: String, trustStoreCredStr: String,
  // connStr : String, userCredStr : String, partitionStr : String): Unit = {
  //
  //    val trustCred = createTrustStoreCredForExecutors(trustStoreCredStr, path)
  //
  //        val conns = connStr.split(":")
  //
  //    val conn = Array(conns(0).toInt, conns(1).toInt)
  //
  //    val usr = userCredStr.split(":")
  //
  //           val webHdfsChkDirOpr = "op=GETFILESTATUS"
  //    val returnChkDir = callWebHdfsAPI(path, "", "GET", "CODE", trustCred, usr, conn,
  // webHdfsChkDirOpr)
  //
  //    if (returnChkDir == "200")
  //            throw new Exception("The File Already Exists : " + path + "\n")
  //
  //    val dPartitions = partitionStr.toInt
  //
  //    val textRdd = dataToWrite.repartition(dPartitions)
  //
  //           val webHdfsMakeDirOpr = "op=MKDIRS"
  //    val returnCreateDir = callWebHdfsAPI(path, "", "PUT", "CODE", trustCred, usr, conn,
  // webHdfsMakeDirOpr)
  //
  //    textRdd.mapPartitionsWithIndex((idx, iter) => WebHdfsConnector.saveAllFiles(idx, iter, usr,
  // path, trustCred, conn)).collect()
  //
  //  }
  //
  //  /**
  //    * This function is passed to mapPartitionsWithIndex so that each executor task can save part
  // of the data using separate connection
  //    */
  //  def saveAllFiles (idx: Int, data : Iterator[String], usrCred: Array[String], path: String,
  // trustCred: Tuple3[String, File , String], connProp: Array[Int]): Iterator[String]  = {
  //
  //
  //    var dataP = data.next()
  //        while(data.hasNext) {
  //            dataP = dataP + "\n" + data.next()
  //        }
  //
  //    val fnameArray = path.split("/")
  //    val fnameIdx = fnameArray.length - 1
  //    val fname = fnameArray(fnameIdx)
  //    val filePath = s"$path/part-000$idx-$fname"
  //
  //           val createOpr = "op=CREATE"
  //
  //    val createUrl = callWebHdfsAPI(filePath, "", "PUT", "LOCATION", trustCred, usrCred,
  // connProp, createOpr)
  //    val created = callWebHdfsAPI(createUrl, dataP, "PUT", "CODE", trustCred, usrCred, connProp,
  // createOpr)
  //
  //    val ret = Array(created.toString)
  //    ret.iterator
  //
  //  }



  def callWebHdfsAPI(path: String,
                     data: String,
                     method: String,
                     respType: String,
                     trustStoreCredStr: String,
                     usrCredStr: String,
                     connStr: String,
                     opr: String,
                     outputType : String): Any = {

    // print("path in callWebHdfs : " + path + "\n")

    val pathComp = path.split(":")

    val trustCred = createTrustStoreCredForExecutors(trustStoreCredStr, path)

    val conns = connStr.split(":")

    val connProp = Array(conns(0).toInt, conns(1).toInt)

    val usrCred = usrCredStr.split(":")

    val uri = (if (trustCred._1 != "") "https:" else "http:") + pathComp(1) + ":" + pathComp(2) +
      "?" + opr

    var httpc = Http(uri).auth(usrCred(0), usrCred(1)).timeout(connTimeoutMs = connProp(0),
      readTimeoutMs = connProp(1))

    httpc = (method : @switch) match {
      case "GET" => httpc
      case "PUT" => httpc.put(data).header("content-type", "application/csv")
      case "POST" => httpc.postData(data).header("content-type", "application/csv")
    }

    httpc = (trustCred._1 : @switch) match {
      case "" => httpc
      case "N" => httpc.option(HttpOptions.allowUnsafeSSL)
      case "Y" => httpc.option(HttpOptions.sslSocketFactory(biocSslSocketFactory(trustCred._2,
        trustCred._3)))
    }

    val out = (outputType : @switch) match {
      case "" => httpc.asString
      case "String" => httpc.asString
      case "Bytes" => httpc.asBytes
    }

    val resp = (respType : @switch) match {
      case "BODY" => out.body
      case "CODE" => out.code
      case "HEADERS" => out.headers
      case "LOCATION" => out.location.mkString(" ")
    }

    /*
    val resp = (respType : @switch) match {
      case "BODY" => httpc.asBytes.body
      case "CODE" => httpc.asString.code
      case "HEADERS" => httpc.asString.headers
      case "LOCATION" => httpc.asString.location.mkString(" ")
    }
    */

    resp
  }

  def getFileInputStream (filePath: String,
                          offset: Long,
                          length: Long,
                          trustStoreCredStr: String,
                          connStr: String,
                          usrCredStr: String): ByteArrayInputStream = {

    // print("path in getFileInputStream : " + filePath + "\n")

    val fileGetOpr = if (length > 0) {
      s"op=OPEN&offset=$offset&length=$length&bufferSize=$length"
    } else {
      s"op=OPEN&offset=$offset"
    }

    val getUrl = callWebHdfsAPI(filePath, "", "GET", "LOCATION", trustStoreCredStr, usrCredStr,
      connStr, fileGetOpr, "String").asInstanceOf[String]

    val content = callWebHdfsAPI(getUrl, "", "GET", "BODY", trustStoreCredStr, usrCredStr,
      connStr, fileGetOpr, "Bytes").asInstanceOf[Array[Byte]]

    new ByteArrayInputStream(content)
  }

  def getFileStatus(filePath: String,
                    trustStoreCredStr: String,
                    connStr: String,
                    usrCredStr: String): Map[String, Any] = {

    // print("path in getFileStatus : " + filePath + "\n")
    val fileStatusOpr = s"op=GETFILESTATUS"

    val fileStatus = callWebHdfsAPI(filePath, "", "GET", "BODY", trustStoreCredStr, usrCredStr,
      connStr, fileStatusOpr, "String").asInstanceOf[String]

    if (fileStatus.contains("RemoteException")) {
      throw new Exception(fileStatus)
    }

    val responseMap = scala.util.parsing.json.JSON.parseFull(fileStatus).toList(0)
      .asInstanceOf[Map[String, Map[String, Any]]]

    responseMap.getOrElse("FileStatus", throw new Exception ("File Status Null"))
  }

  def getListStatus(filePath: String,
                    trustStoreCredStr: String,
                    connStr: String,
                    usrCredStr: String): List[Map[String, Any]] = {
    // print("path in getListStatus : " + filePath + "\n")
    val listStatusOpr = s"op=LISTSTATUS"

    val listStatus = callWebHdfsAPI(filePath, "", "GET", "BODY", trustStoreCredStr, usrCredStr,
      connStr, listStatusOpr, "String").asInstanceOf[String]

    if (listStatus.contains("RemoteException")) {
      throw new Exception(listStatus)
    }

    scala.util.parsing.json.JSON.parseFull(listStatus).toList(0)
      .asInstanceOf[Map[String, Map[String, Any]]].get("FileStatuses").get("FileStatus")
      .asInstanceOf[List[Map[String, Any]]]
  }

}

