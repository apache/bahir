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

package org.apache.bahir.datasource.webhdfs

import java.io._
import java.net.URI

import scala.collection.mutable.HashMap
import scala.math._

import org.apache.bahir.datasource.webhdfs.util.WebHdfsConnector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable


/*
 * This class contains functions for reading/writing data from/to remote webhdfs server in Spark
 * DataSource
 */
// TODO: reenable scalastyle checks
// scalastyle:off println
class BahirWebHdfsFileSystem extends FileSystem {

  println(s" - - - BahirWebHdfsFileSystem class loaded - - - ")

  var uri: URI = null
  var rHdfsUri: URI = null
  var conf: Configuration = null
  var workingDir = null

  var readFullFile = false
  var usrCred = ""
  var connections = 0
  var certValidation = "Y"

  var fileStatusMap: HashMap[String, FileStatus] = HashMap()
  var listStatusMap: HashMap[String, Array[FileStatus]] = HashMap()

  override def getUri(): URI = uri


  /**
   * This method does necessary initialization of the configuration parameters
   */
  override def initialize(uriOrg: URI,
                          confOrg: Configuration): Unit = {

    super.initialize(uriOrg, confOrg)

    setConf(confOrg)

    rHdfsUri = uriOrg
    conf = confOrg

    val rfFlg = conf.get("readFullFile")
    readFullFile = if (rfFlg == null) false else rfFlg.toBoolean

    val usrCredStr = conf.get("usrCredStr")
    usrCred = if (usrCredStr == null) {
      throw new Exception("User Credential Has To Be Specified For The Remote HDFS")
    } else usrCredStr.toString

    val conns = conf.get("connections")
    connections = if (conns == null) 0 else conns.toInt

    val certFlg = conf.get("certValidationFlg")
    certValidation = if (certFlg == null) "Y" else certFlg.toString

    uri = URI.create(uriOrg.getScheme() + "://" + uriOrg.getAuthority())

    println(s"BahirWebHdfsFileSystem: uri=${uri}, connections=${connections}, usercred=${usrCred}")
  }

  override def getWorkingDirectory(): Path = {

    val path = new Path(rHdfsUri)
    // println("Working Directory: " + path)
    path
  }

  override def setWorkingDirectory(dir: Path): Unit = {}

  override def rename(srcPath: Path, destPath: Path): Boolean = {

    //val destPathModStr = destPath.toString.replace("default/gateway/webhdfs/v1/", "")(1)
    val destPathModStr = Path.getPathWithoutSchemeAndAuthority(destPath).toString.replace("/gateway/default/webhdfs/v1","")
    println("In rename  - Path : " + srcPath + " , dest path : " + destPath + " , mod dest path : "+ destPathModStr)
    //println("In rename after mod  - Path : " + srcPathModStr + " , dest path : " + destPathModStr)
    WebHdfsConnector.renameFile(srcPath.toString, destPathModStr, certValidation, "1000:5000", usrCred)
  }

  override def delete(srcPath: Path, recursive: Boolean): Boolean = {
    println("In delete  - Path : " + srcPath + " , recursive flg : " + recursive)
    val srcPathModStr = modifyFilePath(srcPath).toString	
    println("In delete after mod - Path : " + srcPathModStr + " , recursive flg : " + recursive)
    WebHdfsConnector.deleteFile(srcPathModStr, recursive, certValidation, "1000:5000", usrCred)
  }

  override def mkdirs(srcPath: Path, permission: FsPermission): Boolean = {
    println("In MkDirs  - Path : " + srcPath)
    val srcPathModStr = modifyFilePath(srcPath).toString	
    println("In MkDirs  after mod - Path : " + srcPathModStr)
    WebHdfsConnector.makeDirectory(srcPathModStr, permission.toShort, certValidation, "1000:5000", usrCred)
  }

  override def append(srcPath: Path,
                      bufferSize: Int,
                      progress: Progressable): FSDataOutputStream = {
    throw new Exception("File Append Not Supported")
  }

  override def getFileStatus(f: Path): FileStatus = {
    val file = modifyFilePath(f).toString
    println("In bahir get filestatus  - Path , and mod Path: " + f + file + "\n")
    var fStatus: FileStatus = fileStatusMap.getOrElse(f.toString, null)
    println("In bahir after checking filestatus map  - : " + fStatus)

    val fileStatus = if (fStatus == null) {
      println("In bahir before calling webhdfsconnector  : ")
      val fStatusMap = WebHdfsConnector.getFileStatus(file, certValidation, "1000:5000", usrCred)
      if (fStatusMap != null) {
      	fStatus = createFileStatus(f, fStatusMap)
      	fileStatusMap.put(f.toString, fStatus)
      }
      fStatus      
    }
    else {
      fStatus
    }

    println("In bahir before returning from getFileStatis fileStatus  : " + fileStatus)
    fileStatus
  }

  override def listStatus(f: Path): Array[FileStatus] = {

    val file = modifyFilePath(f).toString

    var lStatus: Array[FileStatus] = listStatusMap.getOrElse(f.toString, null)

    // println("file in listStatus: " + file)

    val listStatus = if (lStatus == null) {
      val fStatusMapList = WebHdfsConnector
        .getListStatus(file, certValidation, "1000:5000", usrCred)
      val fileCount = fStatusMapList.length
      lStatus = new Array[FileStatus](fileCount)
      var i = 0
      while (i < fileCount) {
        lStatus(i) = createFileStatus(f, fStatusMapList(i))
        i += 1
      }
      listStatusMap.put(f.toString, lStatus)
      lStatus
    }
    else {
      lStatus
    }

    // println(" listStatus: " + listStatus)
    listStatus
  }

  private def createFileStatus(fPath: Path, statusMap: Map[String, Any]): FileStatus = {

    val lng = conf.get("length")
    val partlng = if (lng == null) 1 else lng.toInt

    val blk = conf.get("block")
    val partblk = if (blk == null) 1 else blk.toInt


    val isDirFlg = if (statusMap.getOrElse("type", "") == "DIRECTORY") true else false
    val pathSuffix = statusMap.getOrElse("pathSuffix", "")
    val targetPath = if (pathSuffix == "") fPath else new Path(fPath.toString + "/" + pathSuffix)
    val fStatus = new FileStatus(
      statusMap.getOrElse("length", 0).asInstanceOf[Double].toLong * partlng,
      isDirFlg,
      statusMap.getOrElse("replication", 1).asInstanceOf[Double].toInt,
      (statusMap.getOrElse("blockSize", 128000000).asInstanceOf[Double].toLong) / partblk,
      statusMap.getOrElse("modificationTime", 0).asInstanceOf[Double].toLong,
      statusMap.getOrElse("accessTime", 0).asInstanceOf[Double].toLong,
      null,
      statusMap.getOrElse("owner", "default").asInstanceOf[String],
      statusMap.getOrElse("group", "default").asInstanceOf[String],
      null, targetPath)
    fStatus
  }

  private def modifyFilePath(f: Path): Path = {
    // println("file uri : " + f.toUri)
    val wQryStr = f.toString.replace(getQryStrFromFilePath(f), "")
    //val fStr = wQryStr.replace("_temporary", "bahir_tmp")	
    new Path(wQryStr)
  }

  private def getQryStrFromFilePath(f: Path): String = {
    val fileStr = f.toString
    val start = fileStr.indexOf("&")
    val end = fileStr.indexOf(";")

    // print("start and end index  " + start +"\n")

    val qryStr = if (start > 0) fileStr.substring(start, end) else ""

    // print("query : " + qryStr + "\n")
    qryStr
  }

  override def open(f: Path, bs: Int): FSDataInputStream = {

    println("In bahir open file path - : " + f)

    val fileStatus = getFileStatus(f)
    val blockSize = fileStatus.getBlockSize
    val fileLength = fileStatus.getLen

    val file = modifyFilePath(f)

    print("file uri in open after modification : " + file + "\n")

    val qMap = getQryMapFromFilePath(f)

    val fConnections = if (qMap == null) {
      connections
    }
    else {
      qMap.getOrElse("connections", connections).asInstanceOf[String].toInt
    }

    new FSDataInputStream(new BahirWebHdfsInputStream(file, bs, blockSize, fileLength,
      readFullFile, usrCred, fConnections, certValidation))
  }

  override def create(srcPath: Path,
                      permission: FsPermission,
                      overwriteFlg: Boolean,
                      bufferSize: Int,
                      replication: Short,
                      blockSize: Long,
                      progress: Progressable): FSDataOutputStream = {

    val file = modifyFilePath(srcPath)

    print("file uri in create after modification : " + file + "\n")

    new FSDataOutputStream(new BahirWebHdfsOutputStream(file, bufferSize, blockSize,
      permission.toShort, replication, overwriteFlg, usrCred, certValidation), null)
  }

  private def getQryMapFromFilePath(f: Path): HashMap[String, String] = {

    val qryStr = getQryStrFromFilePath(f)
    if (qryStr == "") null
    else {

      val params = qryStr.replace(";", "").substring(1).split("&")

      val paramCount = params.length

      // print("params : " + params + " , lenth : " + paramCount + "\n")
      var paramMap: HashMap[String, String] = new HashMap()

      var i = 0

      while (i < paramCount) {
        val paramKV = params(i).split("=")
        paramMap.put(paramKV(0), paramKV(1))
        i += 1
      }

      // print("param map : " + paramMap + "\n")
      paramMap
    }

  }

}

class BahirWebHdfsInputStream(fPath: Path,
                              bufferSz: Int,
                              blockSz: Long,
                              fileSz: Long,
                              readFull: Boolean,
                              usrCrd: String,
                              conns: Int,
                              certValidation: String)
  extends FSInputStream {

  val filePath: Path = fPath
  val bufferSize: Int = bufferSz
  val blockSize: Long = blockSz
  val fileSize: Long = fileSz
  val readFullFlg: Boolean = readFull
  val usrCred: String = usrCrd
  val connections: Int = conns
  val certValidationFlg: String = certValidation

  var pos = 0L

  var in: ByteArrayInputStream = null

  var callCount = 0

  /*
   * This is a dummy implementation as Spark does not use it. We need it here just to satisy
   * interface contract
   */
  override def read(): Int = {
    read(new Array[Byte](4056), 0, 100)
  }

  override def read(b: Array[Byte], offset: Int, length: Int): Int = {
//    if (in == null) createWebHdfsInputStream(pos)
    callCount += 1
    var bCount = in.read(b, offset, length)

//    println("In read - call count: " + callCount + " , pos: " + pos + ", offset: " + offset +
//      ", length: " + length + ", byte count total: " + bCount)

    if (bCount < 0 && pos < fileSize) {
//      println("In read - bCount less than 0, call count: " + callCount + ", file size : " +
//        fileSize + " , pos : " + pos + ", offset : " + offset + " , length : " + length +
//        " , byte count total : " + bCount)
      // createWebHdfsInputStream(pos)
      seek(pos)
      bCount = in.read(b, offset, length)
    }

    pos += bCount

    bCount

  }

  override def seek(newPos: Long): Unit = {
    // print("In seek -  newpos : " + newPos + " , old pos : " + pos + "\n")
    if (pos != newPos) {
      pos = newPos
      if (in != null) in.close
    }
    createWebHdfsInputStream(pos)
  }

  private def createWebHdfsInputStream(pos: Long) = {

    val poe = if (connections == 0) {
      if (blockSize > fileSize || readFullFlg == true) {
        fileSize
      } else {
        (floor(pos / blockSize).toLong + 1) * blockSize + 10000
      }
    }
    else {
      floor(fileSize / (connections - 1)).toInt + 10000
    }


    // println("In read - input stream null , block size : "  + blockSize +  " , file size : " +
    // fileSize +  " , red full flg : " + readFullFlg + " , pos : " + pos + " , poe : " + poe +"\n")

    in = WebHdfsConnector
      .getFileInputStream(filePath.toString(), pos, poe, certValidationFlg, "1000:50000", usrCred)
  }

  /*
   * This is a dummy implementation as Spark does not use it. We need it here just to satisy
   * interface contract
   */
  override def seekToNewSource(targetPos: Long): Boolean = false

  override def getPos(): Long = pos

}


class BahirWebHdfsOutputStream(fPath: Path,
                               bufferSz: Int,
                               blockSz: Long,
                               perms: Short,
                               replicationCnt: Short,
                               overwriteFlg: Boolean,
                               usrCrd: String,
                               certValidation: String)
  extends OutputStream {

  val filePath: Path = fPath
  val bufferSize: Int = bufferSz
  val blockSize: Long = blockSz
  val permission: Short = perms
  val replication: Short = replicationCnt
  val overwrite: Boolean = overwriteFlg
  val usrCred: String = usrCrd
  val certValidationFlg: String = certValidation


  override def write(b: Int): Unit = {

    println("in write single byte: " + b)
    val singleByte : Array[Byte] = new Array(b)(1)
    writeBytes(singleByte)
  }

  override def write(b: Array[Byte]): Unit = {
    println("in write bytes ")
    writeBytes(b)
  }

  override def write(b: Array[Byte], offset: Int, length: Int): Unit = {
    println("in write bytes with offset and length : " + offset + " , " + length)
    writeBytes(b)
  }

  private def writeBytes(b: Array[Byte]): Unit = {
    println("in provate write bytes ")
    WebHdfsConnector.writeFile(b, filePath.toString, permission, overwriteFlg, bufferSize,
      replication, blockSize, certValidation, "1000:5000", usrCred)
  }

}
// TODO: reenable scalastyle checks
// scalastyle:on println

