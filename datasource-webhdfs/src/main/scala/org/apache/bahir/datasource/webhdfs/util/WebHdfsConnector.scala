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
import java.net.{HttpURLConnection, URL}
import java.security._
import javax.net.ssl.{SSLContext, SSLSocketFactory, TrustManagerFactory}

import scala.annotation.switch

import scalaj.http.{Http, HttpOptions}

/**
 * This object contains all utility functions for reading/writing data from/to remote webhdfs
 * server. The abstraction maintained in this layer is at the level of RDD
 */
object WebHdfsConnector {

  /*
   * This function returns a Tuple for credential store which contains flag for validating
   * Certificate, the Certificate File object and Certificate File Object password
   * the value of the parameter cred is "N" it ignores certificate validation, if
   * it is "Y" then it downloads a certificate using openssl, else it expects a valid
   * path for trust store and password for using the same
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
          val tsd = cred.split(":")
          new Tuple3("Y", new java.io.File(tsd(0)), tsd(1))
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

  /**
   * This function creates a directory in remote HDFS
   */
  def makeDirectory(path: String,
                    permission: Short,
                    trustStoreCredStr: String,
                    connStr: String,
                    userCredStr: String): Boolean = {
    /*
    val webHdfsChkDirOpr = "op=GETFILESTATUS"
    val returnChkDir = callWebHdfsAPI(path, null, "GET", "CODE", trustStoreCredStr, userCredStr,
      connStr, webHdfsChkDirOpr, "String").asInstanceOf[Integer]
    */

    val webHdfsMakeDirOpr = s"op=MKDIRS&permission=$permission"
    val returnMakeDir = callWebHdfsAPI(path, "".getBytes(), "PUT", "CODE",
        trustStoreCredStr, userCredStr,
        connStr, webHdfsMakeDirOpr, "String").asInstanceOf[Integer]
    if (returnMakeDir != 200) {
        throw new Exception("The Directory could not be created , Src path,Code: "
          + path + " , " + returnMakeDir + "\n")
    } else true
  }


  /**
   * This function deletes a file/directory recursively  in remote HDFS
   */
  def deleteFile(path: String,
                 recursiveFlg: Boolean,
                 trustStoreCredStr: String,
                 connStr: String,
                 userCredStr: String): Boolean = {

    /*
    val webHdfsChkDirOpr = "op=GETFILESTATUS"
    val returnChkDir = callWebHdfsAPI(path, null, "GET", "CODE", trustStoreCredStr, userCredStr,
      connStr, webHdfsChkDirOpr, "String").asInstanceOf[Integer]
    */

    val webHdfsDeleteDirOpr = s"op=DELETE&recursive=$recursiveFlg"
    val returnDelDir = callWebHdfsAPI(path, null, "DELETE", "CODE", trustStoreCredStr, userCredStr,
        connStr, webHdfsDeleteDirOpr, "String").asInstanceOf[Integer]
    if (returnDelDir != 200) {
        throw new Exception("The File/Directory could not be renamed , Src path,Dest path,Code: "
            + path + " , " + returnDelDir + "\n")
    } else true
  }

  /**
   * This function writes 1 file in remote HDFS
   */
  def writeFile(data: Array[Byte],
                path: String,
                permission: Short,
                overwriteFlg: Boolean,
                bufferSize: Int,
                replication: Short,
                blockSize: Long,
                trustStoreCredStr: String,
                connStr: String,
                userCredStr: String): Boolean = {

    /*
    val webHdfsChkFileOpr = "op=GETFILESTATUS"
    val returnChkFile = callWebHdfsAPI(path, null, "GET", "CODE", trustStoreCredStr, userCredStr,
      connStr, webHdfsChkFileOpr, "String").asInstanceOf[Integer]

    val retCode = if (returnChkFile == 200) {
           // print("in writeFile, file exists : createdCode : " + returnChkFile + "\n")
           true
    } else {
    */

           val webHdfsCreateOpr = s"op=CREATE&overwrite=$overwriteFlg&blockSize=$blockSize" +
              s"&replication=$replication&bufferSize=$bufferSize" +
              s"&permission=$permission"
           val createUrl = callWebHdfsAPI(path, "".getBytes(), "PUT", "LOCATION",
              trustStoreCredStr, userCredStr,
              connStr, webHdfsCreateOpr, "String").asInstanceOf[String]
           val createdCode = callWebHdfsAPI(createUrl, data, "PUT", "CODE", trustStoreCredStr,
             userCredStr, connStr, webHdfsCreateOpr, "String").asInstanceOf[Integer]

           // print("in writeFile, creaedCode : " + createdCode + "\n")

           true
    /*
    }

    retCode
    */

  }

  /**
   * This function renames 1 file in remote HDFS
   */
  def renameFile(path: String,
                 destPath: String,
                 trustStoreCredStr: String,
                 connStr: String,
                 userCredStr: String): Boolean = {
    /*
    val webHdfsChkFileOpr = "op=GETFILESTATUS"
    val returnChkFile = callWebHdfsAPI(path, null, "GET", "CODE", trustStoreCredStr, userCredStr,
      connStr, webHdfsChkFileOpr, "String").asInstanceOf[Integer]
    */

    val webHdfsRenameOpr = s"op=RENAME&destination=$destPath"
    val returnRename = callWebHdfsAPI(path, "".getBytes(), "PUT", "CODE",
        trustStoreCredStr, userCredStr,
        connStr, webHdfsRenameOpr, "String").asInstanceOf[Integer]
    if (returnRename != 200) {
        throw new Exception("The File/Directory could not be renamed , Src path,Dest path,Code: "
           + path + " , " + destPath + " , " + returnRename + "\n")
    }
    else {
      true
    }
  }



  def callWebHdfsAPI(path: String,
                     data: Array[Byte],
                     method: String,
                     respType: String,
                     trustStoreCredStr: String,
                     usrCredStr: String,
                     connStr: String,
                     opr: String,
                     outputType: String): Any = {

    // print("path in callWebHdfs : " + path + " , opr : " + opr + "\n")

    val pathComp = path.split(":")

    val trustCred = createTrustStoreCredForExecutors(trustStoreCredStr, path)

    val conns = connStr.split(":")

    val connProp = Array(conns(0).toInt, conns(1).toInt)

    val usrCred = usrCredStr.split(":")

    val uri = (if (trustCred._1 != "") "https:" else "http:") + pathComp(1) + ":" + pathComp(2) +
      "?" + opr

    var httpc = Http(uri).auth(usrCred(0), usrCred(1)).timeout(connTimeoutMs = connProp(0),
      readTimeoutMs = connProp(1))

    httpc = (method: @switch) match {
      case "GET" => httpc
      case "PUT" => httpc.put(data).header("content-type", "application/bahir-webhdfs")
      case "DELETE" => httpc.method("DELETE")
      case "POST" => httpc.postData(data).header("content-type", "application/bahir-webhdfs")
    }

    httpc = (trustCred._1: @switch) match {
      case "" => httpc
      case "N" => httpc.option(HttpOptions.allowUnsafeSSL)
      case "Y" => httpc.option(HttpOptions.sslSocketFactory(biocSslSocketFactory(trustCred._2,
        trustCred._3)))
    }

    val resp = (respType : @switch) match {
      case "BODY" => httpc.asString.body
      case "BODY-BYTES" => httpc.asBytes.body
      case "BODY-STREAM" => getBodyStream(httpc)
      case "CODE" => httpc.asString.code
      case "HEADERS" => httpc.asString.headers
      case "LOCATION" => httpc.asString.location.mkString(" ")
    }

    resp
  }

  private def getBodyStream(httpReq: scalaj.http.HttpRequest) : InputStream = {

    val conn = (new URL(httpReq.urlBuilder(httpReq))).openConnection.asInstanceOf[HttpURLConnection]

    HttpOptions.method(httpReq.method)(conn)

    httpReq.headers.reverse.foreach{ case (name, value) =>
          conn.setRequestProperty(name, value)
    }

    httpReq.options.reverse.foreach(_(conn))

    httpReq.connectFunc(httpReq, conn)

    conn.getInputStream

  }

  def getFileInputStream(filePath: String,
                         streamFlg: Boolean,
                         bufferSize: Long,
                         offset: Long,
                         end: Long,
                         trustStoreCredStr: String,
                         connStr: String,
                         usrCredStr: String): InputStream = {

    // print("path in getFileInputStream : " + filePath + " , bufferSize : " + bufferSize + "\n")

    var length = 0L

    val fileGetOpr = if (end > 0) {
      length = end - offset
      s"op=OPEN&offset=$offset&length=$length&bufferSize=$bufferSize"
    } else {
      s"op=OPEN&offset=$offset&bufferSize=$bufferSize"
    }

    val getUrl = callWebHdfsAPI(filePath, null, "GET", "LOCATION", trustStoreCredStr, usrCredStr,
      connStr, fileGetOpr, "String").asInstanceOf[String]

    if (streamFlg == true) {
        callWebHdfsAPI(getUrl, null, "GET", "BODY-STREAM",
           trustStoreCredStr, usrCredStr,
           connStr, fileGetOpr, "Stream").asInstanceOf[InputStream]
    } else {
        val content = callWebHdfsAPI(getUrl, null, "GET", "BODY-BYTES",
           trustStoreCredStr, usrCredStr,
           connStr, fileGetOpr, "Stream").asInstanceOf[Array[Byte]]
        new ByteArrayInputStream(content)
    }

  }

  def getFileStatus(filePath: String,
                    trustStoreCredStr: String,
                    connStr: String,
                    usrCredStr: String): Map[String, Any] = {

    val fileStatusOpr = s"op=GETFILESTATUS"
    val returnChk = callWebHdfsAPI(filePath, null, "GET", "CODE", trustStoreCredStr, usrCredStr,
      connStr, fileStatusOpr, "String").asInstanceOf[Integer]
    // print("after file status check in getFileStatus in WebHdfsConnector : " + returnChk + "\n")

    if (returnChk == 200) {

      // print("within return code 200 in getFileStatus : " + returnChk + "\n")
      val fileStatus = callWebHdfsAPI(filePath, null, "GET", "BODY", trustStoreCredStr, usrCredStr,
        connStr, fileStatusOpr, "String").asInstanceOf[String]

      if (fileStatus.contains("RemoteException")) {
        null
      }
      else {
        val responseMap = scala.util.parsing.json.JSON.parseFull(fileStatus).toList(0)
              .asInstanceOf[Map[String, Map[String, Any]]]

        responseMap.getOrElse("FileStatus", null)
      }
    }
    else {
      null
    }
  }

  def getListStatus(filePath: String,
                    trustStoreCredStr: String,
                    connStr: String,
                    usrCredStr: String): List[Map[String, Any]] = {
    val listStatusOpr = s"op=LISTSTATUS"

    val listStatus = callWebHdfsAPI(filePath, null, "GET", "BODY", trustStoreCredStr, usrCredStr,
      connStr, listStatusOpr, "String").asInstanceOf[String]

    if (listStatus.contains("RemoteException")) {
      throw new Exception(listStatus)
    }

    scala.util.parsing.json.JSON.parseFull(listStatus).toList(0)
      .asInstanceOf[Map[String, Map[String, Any]]].get("FileStatuses").get("FileStatus")
      .asInstanceOf[List[Map[String, Any]]]
  }

}
