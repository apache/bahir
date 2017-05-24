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
import java.nio.charset.Charset
import java.security._
import javax.net.ssl.{SSLContext, SSLSocketFactory, TrustManagerFactory}

import scala.annotation.switch
import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.util.parsing.json.JSON

import scalaj.http._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * This object contains all utility functions for reading/writing data from/to remote WebHDFS
 * server. The abstraction maintained in this layer is at the level of RDD.
 */
private[webhdfs] object WebHdfsConnector {

  /**
   * Currently only files transferred using UTF-8 are supported
   */
  val DEFAULT_CHARSET = Charset.forName("UTF-8")

  /**
   * This function prepares the partition details for each file based on the details populated by
   * getFilesDetails. This partition details is further used to spawn multiple connections to get
   * data of a file using multiple connections.
   */
  def preparePartitions(
      fileDetails: Array[(String, Long, Long, Int)],
      baseFile: String,
      partitionDetails: String,
      recordSeparator: String): Array[(String, Long, Long, Int, Int, Int, String)] = {

    val totalFileCount = fileDetails.length
    var i = 0

    val partitionDet = partitionDetails.split(":")

    // if number of partitions used for opening connections is passed as 0 or less, partition is
    // defaulted to 4
    var filePartition = if (partitionDet(0).toInt < 1) 4 else partitionDet(0).toInt

    // if partition span (used to resolve record boundary) is sent as less than 10 KB, it is
    // defaulted to 10 KB. Otherwise it is kept between 10KB to 100 KB
    var partitionSpan = if (partitionDet(1).toInt < 10000) {
      10000
    } else {
      math.min(partitionDet(1).toInt, 100000)
    }

    i = 0

    var partList = new ArrayBuffer[(String, Long, Long, Int, Int, Int, String)]()

    var j = 0
    var k = 0
//    var filePart = 0
    var partPath = ""
    var partLength = 0L
    var fileLength = 0L
    var partOffset = 0L
//    var fileSpan = 0

//    val maxSpan = 1000000
//    val minSpan = 1000

    while (i < totalFileCount) {

      fileLength = fileDetails(i)._2.toLong

      if (fileLength > 0) {

        partPath = fileDetails(i)._1

        fileLength = fileDetails(i)._2.toLong
        partLength = fileLength / filePartition

        if (partLength < 1000000) {
          filePartition = 1
          partitionSpan = 0
        }

        j = 0
        while (j < filePartition) {
          partOffset = j * partLength.toLong

          if (j + 1 == filePartition) partLength = fileDetails(i)._2.toLong - j * partLength

          partList += new Tuple7(partPath, partOffset, partLength, j + 1, filePartition,
            partitionSpan, recordSeparator)
          j += 1
          k += 1
        }
      }

      i += 1

    }

    if (k < 1) {
      throw new Exception("Zero File Content")
    }

    var finalPartList = new Array[Tuple7[String, Long, Long, Int, Int, Int, String]](k)

    partList.copyToArray(finalPartList)

    finalPartList
  }

  /**
   * This function returns the list of files in a folder with file details as RDD
   */
  def listFromWebHdfs(sc: SparkContext,
                      path: String,
                      trustStoreCred: String,
                      userCred: String,
                      connProp: String): RDD[String] = {

    val conns = connProp.split(":")

    val conn = Array(conns(0).toInt, conns(1).toInt)

    val usrCred = userCred.split(":")

    val trustCred = createTrustStoreCredForExecutors(trustStoreCred, path)

    val fileDetails = getFilesDetails(path, trustCred, usrCred, conn)

    def g(v: (String, Long, Long, Int)) = {
      v._1.split("/").last + "," + v._2.toString + "," + v._3.toString + "," + v._4.toString + "\n"
    }

    val fds = fileDetails.map(x => g(x))

    val flRdd = sc.parallelize(fds)

    flRdd
  }

  /**
   * This function returns a Tuple for credential store which contains flag for validating
   * Certificate, the Certificate File object and Certificate File Object password.
   */
  def createTrustStoreCredForExecutors(cred: String, path: String): Tuple3[String, File, String] = {

    val trustStoreMap = if (cred != "") {
      if (cred == "N") {
        new Tuple3("N", null, "")
      }
      else {
        if (cred == "Y") {
          val tsd = SSLTrustStore.getCertDetails(path)
          new Tuple3("Y", tsd._1, tsd._2)
        }
        else {
          throw new Exception("Invalid Certificate Validation Option")
        }
      }
    } else {
      new Tuple3("", null, "")
    }
    trustStoreMap
  }

  /**
   * This function returns the details of the the files in a folder if the path passed is a folder.
   * In case a File path is passed it returns the details of the files.
   * Returns an Array of Tuple where each Tuple represents one file with details of full file path,
   * size of the file, block size of the file and number of partitions based on size of the file and
   * block size.
   */
  def getFilesDetails(path: String,
                      trustCred: (String, File, String),
                      usrCred: Array[String],
                      connProp: Array[Int]): Array[(String, Long, Long, Int)] = {

    val listStatusOpr = s"op=LISTSTATUS"

    val listStatus = callWebHdfsAPI(path, "", "GET", "BODY", trustCred, usrCred, connProp,
      listStatusOpr)

    if (listStatus.contains("RemoteException")) {
      throw new Exception(listStatus)
    }

    val flist = JSON
      .parseFull(listStatus)
      .toList(0)
      .asInstanceOf[Map[String, Map[String, Any]]]
      .get("FileStatuses")
      .get("FileStatus")
      .asInstanceOf[List[Map[String, Any]]]

    val fileCount = flist.length

    var i = 0
    var j = 0L
    val fileDetails = new Array[(String, Long, Long, Int)](fileCount)
    var fSuffix = ""
    var fLength = 0L
    var fBlocksize = 0L
    var fPart = 0
    var fullFilePath = ""

    while (i < fileCount) {
      fSuffix = flist(i).get("pathSuffix").getOrElse(path).asInstanceOf[String].toString

      fullFilePath = if (fSuffix == "") path else (path + "/" + fSuffix)

      fLength = flist(i).get("length").getOrElse(0).asInstanceOf[Double].toLong

      fBlocksize = flist(i).get("blockSize").getOrElse(0).asInstanceOf[Double].toLong
      if (fLength > 0) fPart = (floor((fLength / fBlocksize)).toInt + 1) else fPart = 0
      fileDetails(i) = new Tuple4(fullFilePath, fLength, fBlocksize, fPart)

      i += 1
    }
    fileDetails
  }

  /**
   * This function calls the WebHDFS API after creating all necessary parameters from different
   * configurations
   */
  def callWebHdfsAPI(path: String,
                     data: String,
                     method: String,
                     respType: String,
                     trustStoreCred: (String, File, String),
                     userCred: Array[String],
                     connProp: Array[Int],
                     opr: String): String = {

    val pathComp = path.split(":")

    val uri = (if (trustStoreCred._1 != "") "https:" else "http:")
      .concat(pathComp(1) + ":" + pathComp(2) + "?" + opr)

    var httpc = Http(uri).auth(userCred(0), userCred(1)).timeout(connTimeoutMs = connProp(0),
      readTimeoutMs = connProp(1))

    httpc = (method: @switch) match {
      case "GET" => httpc
      case "PUT" => httpc.put(data).header("content-type", "application/csv")
      case "POST" => httpc.postData(data).header("content-type", "application/csv")
    }

    httpc = (trustStoreCred._1: @switch) match {
      case "" => httpc
      case "N" => httpc.option(HttpOptions.allowUnsafeSSL)
      case "Y" => httpc.option(HttpOptions.sslSocketFactory(biocSslSocketFactory(trustStoreCred._2,
        trustStoreCred._3)))
    }

    val resp = (respType: @switch) match {
      case "BODY" => httpc.asString.body
      case "CODE" => httpc.asString.code
      case "HEADERS" => httpc.asString.headers
      case "LOCATION" => httpc.asString.location.mkString(" ")
    }

    resp.toString()
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
   * This function returns data of a file (or data of all files in a folder with same structure) as
   * RDD
   */
  def loadFromWebHdfs(sc: SparkContext,
                      path: String,
                      charset: String,
                      trustStoreCred: String,
                      userCred: String,
                      connProp: String,
                      partitionDetails: String,
                      recordSeparator: String): RDD[String] = {

    val conns = connProp.split(":")

    val conn = Array(conns(0).toInt, conns(1).toInt)

    val usrCrd = userCred.split(":")

    val trustCred = createTrustStoreCredForExecutors(trustStoreCred, path)

    val fileDetails = getFilesDetails(path, trustCred, usrCrd, conn)

    val parts = preparePartitions(fileDetails, path, partitionDetails, recordSeparator)

    val input = sc.parallelize(parts, parts.length)

    input.collect()

    val fRdd = input.flatMap(x => WebHdfsConnector.getAllFiles(x, usrCrd, trustCred, conn))

    fRdd
  }

  /**
   * This function is passed to each executor through flatMap function to spawn one http connection
   * from each executor for get a part of the file
   */
  def getAllFiles(partInfo: (String, Long, Long, Int, Int, Int, String),
                  usrCred: Array[String],
                  trustCred: (String, File, String),
                  connProp: Array[Int]): Iterator[String] = {

    val foffset = partInfo._2.toLong
    val flength = partInfo._3.toLong
    val ffilePath = partInfo._1
    val fpartNum = partInfo._4
    val ftotalPart = partInfo._5
    val fspan = partInfo._6
    val frecordSeparator = partInfo._7

    val fileGetOpr = if (fpartNum < ftotalPart) {
      val effLength = flength + fspan
      s"op=OPEN&offset=$foffset&length=$effLength&bufferSize=$effLength"
    }
    else {
      s"op=OPEN&offset=$foffset&length=$flength&bufferSize=$flength"
    }

    val getUrl = callWebHdfsAPI(ffilePath, "", "GET", "LOCATION", trustCred, usrCred, connProp,
      fileGetOpr)
    val partContent = callWebHdfsAPI(getUrl, "", "GET", "BODY", trustCred, usrCred, connProp,
      fileGetOpr)

    val records = getTillEndOfRecord(partContent, flength, fpartNum, ftotalPart, frecordSeparator)

    records.split("\n").iterator
  }

  /**
   * This function resolves record boundaries.
   * Right now this only supports "\n" as record boundary . This function has to be refined to
   * support json or xml formats for different type of record separators
   */
  def getTillEndOfRecord(content: String,
                         partLength: Long,
                         partNum: Int,
                         totalPart: Int,
                         recordSeparator: String): String = {

    val contentBytes = content.getBytes("UTF-8")
//    val recordSeparatorBytes = recordSeparator.getBytes("UTF-8")

    val contentBytesLength = contentBytes.length

    var bytePosition = 0

    var startbyte = 0

    startbyte = if (partNum == 1) 0
    else {
      // TODO: This part of the code has to be rewritten later on to make it more generic for
      // supporting other formats apart from csv. Right now it supports only csv
      while (contentBytes(bytePosition) != '\n') bytePosition += 1
      bytePosition
    }

    val length = if (partNum == totalPart) (contentBytesLength.toInt - startbyte).toInt
    else {
      bytePosition = partLength.toInt
      // TODO: This part of the code has to be rewritten later on to make it more generic for
      // supporting other formats apart from csv. Right now it supports only csv.
      while (contentBytes(bytePosition) != '\n') bytePosition += 1
      (bytePosition - startbyte)
    }

    new String(contentBytes, startbyte, length, "UTF-8")
  }

  /**
   * This function writes data back to hdfs using WebHDFS using multiple parallel connections.
   * Right now file overwrite is not supported
   */
  def writeToWebHdfs(dataToWrite: RDD[String],
                     path: String,
                     trustStoreCredStr: String,
                     connStr: String,
                     userCredStr: String,
                     partitionStr: String): Unit = {

    val trustCred = createTrustStoreCredForExecutors(trustStoreCredStr, path)

    val conns = connStr.split(":")

    val conn = Array(conns(0).toInt, conns(1).toInt)

    val usr = userCredStr.split(":")

    val webHdfsChkDirOpr = "op=GETFILESTATUS"
    val returnChkDir = callWebHdfsAPI(path, "", "GET", "CODE", trustCred, usr, conn,
      webHdfsChkDirOpr)

    if (returnChkDir == "200") {
      throw new Exception("The File Already Exists : " + path + "\n")
    }

    val dPartitions = partitionStr.toInt

    val textRdd = dataToWrite.repartition(dPartitions)

    val webHdfsMakeDirOpr = "op=MKDIRS"
    val returnCreateDir = callWebHdfsAPI(path, "", "PUT", "CODE", trustCred, usr, conn,
      webHdfsMakeDirOpr)

    textRdd.mapPartitionsWithIndex((idx, iter) => WebHdfsConnector.saveAllFiles(idx, iter, usr,
      path, trustCred, conn)).collect()
  }

  /**
   * This function is passed to mapPartitionsWithIndex so that each executor task can save part of
   * the data using separate connection
   */
  def saveAllFiles(idx: Int,
                   data: Iterator[String],
                   usrCred: Array[String],
                   path: String,
                   trustCred: (String, File, String),
                   connProp: Array[Int]): Iterator[String] = {

    var dataP = data.next()
    while (data.hasNext) {
      dataP = dataP + "\n" + data.next()
    }

    val fnameArray = path.split("/")
    val fnameIdx = fnameArray.length - 1
    val fname = fnameArray(fnameIdx)
    val filePath = s"$path/part-000$idx-$fname"

    val createOpr = "op=CREATE"

    val createUrl = callWebHdfsAPI(filePath, "", "PUT", "LOCATION", trustCred, usrCred, connProp,
      createOpr)
    val created = callWebHdfsAPI(createUrl, dataP, "PUT", "CODE", trustCred, usrCred, connProp,
      createOpr)

    val ret = Array(created.toString)
    ret.iterator
  }
}

