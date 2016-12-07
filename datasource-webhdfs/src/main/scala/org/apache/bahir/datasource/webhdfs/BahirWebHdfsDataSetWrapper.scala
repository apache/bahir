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

import java.net.URI
import java.net.URL
import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import scala.collection.mutable.HashMap
import scala.math._

import org.apache.bahir.datasource.webhdfs.util._
import org.apache.bahir.datasource.webhdfs.csv._


/**
	* This class contains functions for reading/writing data from/to remote webhdfs server in Spark DataSource
*/



class BahirWebHdfsFileSystem
  extends FileSystem {

 	var uri:URI = null
 	var rHdfsUri:URI = null
 	var conf:Configuration = null
	var workingDir = null

	var readFullFile = false
	var usrCred = ""
	var connections = 0
	var certValidation = "Y"

    	var fileStatusMap : HashMap[String, FileStatus] = HashMap()
    	var listStatusMap : HashMap[String, Array[FileStatus]] = HashMap()

  	override def getUri() : URI = uri


	/**
		* This method does necessary initialization of the configuration parameters
	*/

  	override def initialize(
      		uriOrg: URI,
      		confOrg: Configuration): Unit = {
	
		super.initialize(uriOrg, confOrg)

	 	setConf(confOrg)

		rHdfsUri = uriOrg
		conf = confOrg

		val rfFlg = conf.get("readFullFile") 
		readFullFile = if(rfFlg == null) false else rfFlg.toBoolean

		val usrCredStr = conf.get("usrCredStr") 
		usrCred = if(usrCredStr == null) throw new Exception ("User Credential Has To Be Specified For The Remote HDFS") else usrCredStr.toString

		val conns = conf.get("connections") 
		connections = if(conns == null) 0 else conns.toInt

		val certFlg = conf.get("certValidationFlg") 
		certValidation = if(certFlg == null) "Y" else certFlg.toString

		uri = URI.create(uriOrg.getScheme() + "://" + uriOrg.getAuthority())	
		//print("uri : ", uri +  " , connections : " + connections + " , user cred : " + usrCred + "\n")

  	}

  	override def getWorkingDirectory() : Path = {

    		val path = new Path(rHdfsUri)
    		//print("Working Directory : " + path + "\n")
		path
  	}

  	override def setWorkingDirectory(dir : Path) : Unit = {}

  	override def rename(srcPath : Path, destPath : Path) : Boolean = {
        	throw new Exception("File Rename Not Supported")
  	}

  	override def delete(srcPath : Path, recursive : Boolean) : Boolean = {
        	throw new Exception("File Delete Not Supported")
  	}

  	override def mkdirs(srcPath : Path, permission : FsPermission) : Boolean = {
        	throw new Exception("Make Directory Not Supported")
  	}

  	override def append(srcPath : Path, bufferSize : Int, progress : Progressable) : FSDataOutputStream = {
        	throw new Exception("File Append Not Supported")
  	}

  	override def getFileStatus(f : Path) : FileStatus = {

		val file = stripQryFromFilePath(f).toString

		var fStatus : FileStatus  = fileStatusMap.getOrElse(file, null)	

		val fileStatus = if (fStatus == null) {
			val fStatusMap = WebHdfsConnector.getFileStatus(file,  certValidation, "1000:5000", usrCred)
			fStatus = createFileStatus(f, fStatusMap)
			fileStatusMap.put(f.toString, fStatus)
			fStatus
		}
		else 
			fStatus

		fileStatus
  	}

  	override def listStatus(f : Path) : Array[FileStatus] = {

		val file = stripQryFromFilePath(f).toString

		var lStatus : Array[FileStatus]  = listStatusMap.getOrElse(file, null)	

		//print("file in listStatus: " + file + "\n")

		val listStatus = if(lStatus == null) {

			val fStatusMapList = WebHdfsConnector.getListStatus(file, certValidation, "1000:5000", usrCred)
			val fileCount = fStatusMapList.length

			lStatus = new Array[FileStatus](fileCount)

			var i = 0

			while(i < fileCount)
			{
    				lStatus(i) = createFileStatus(f, fStatusMapList(i))
				i+=1
			}

			listStatusMap.put(f.toString, lStatus)
			lStatus
		}
		else
			lStatus


		//print(" listStatus: " + listStatus + "\n")
		listStatus
  	}

	override def open(f: Path, bs: Int) : FSDataInputStream = {

		val fileStatus = getFileStatus(f)
		val blockSize = fileStatus.getBlockSize
		val fileLength = fileStatus.getLen

		val file = stripQryFromFilePath(f)

		print("file uri in open : " + file + "\n")

		val qMap = getQryMapFromFilePath(f)

		val fConnections = if(qMap == null) connections
		else
			qMap.getOrElse("connections", connections).asInstanceOf[String].toInt

		new FSDataInputStream(new BahirWebHdfsInputStream(file, bs, blockSize, fileLength, readFullFile, usrCred, fConnections, certValidation))

	}

  	override def create(srcPath : Path, permission : FsPermission, flag : Boolean, bufferSize : Int, replication : Short, blockSize : Long, progress : Progressable) : FSDataOutputStream = {
        	throw new Exception("File Create Not Yet Supported")
  	}


	private def createFileStatus(fPath : Path, statusMap: Map[String, Any]) : FileStatus = {
	
		val lng = conf.get("length") 
		val partlng = if(lng == null) 1 else lng.toInt

		val blk = conf.get("block") 
		val partblk = if(blk == null) 1 else blk.toInt


        	val isDirFlg = if(statusMap.getOrElse("type", "") == "DIRECTORY") true else false
        	val pathSuffix = statusMap.getOrElse("pathSuffix", "")
        	val targetPath = if(pathSuffix == "") fPath else new Path(fPath.toString + "/" + pathSuffix)
		val fStatus = new FileStatus(
				statusMap.getOrElse("length", 0).asInstanceOf[Double].toLong*partlng, 
				isDirFlg, 
				statusMap.getOrElse("replication",1).asInstanceOf[Double].toInt,
        			(statusMap.getOrElse("blockSize", 128000000).asInstanceOf[Double].toLong)/partblk, 
				statusMap.getOrElse("modificationTime",0).asInstanceOf[Double].toLong, 
				statusMap.getOrElse("accessTime",0).asInstanceOf[Double].toLong, 
        			null, 
				statusMap.getOrElse("owner", "default").asInstanceOf[String], 
				statusMap.getOrElse("group", "default").asInstanceOf[String], 
				null, targetPath)
		fStatus
	}

	private def stripQryFromFilePath(f : Path) : Path = {

		//print("file uri : " + f.toUri + "\n")

		val pathStrWithoutQry = f.toString.replace(getQryStrFromFilePath(f), "")
		new Path(pathStrWithoutQry)

	}

	private def getQryMapFromFilePath(f : Path) : HashMap[String, String] = {

		val qryStr = getQryStrFromFilePath(f) 
		if(qryStr == "") null 
		else {

			val params = qryStr.replace(";", "").substring(1).split("&")

			val paramCount = params.length

			//print("params : " + params + " , lenth : " + paramCount + "\n")
			var paramMap : HashMap[String, String]  = new HashMap()

			var i = 0

			while(i < paramCount)
			{
				val paramKV = params(i).split("=")
    				paramMap.put(paramKV(0), paramKV(1))
				i+=1
			}

			//print("param map : " + paramMap + "\n")
			paramMap
		}

	}

	private def getQryStrFromFilePath(f : Path) : String = {

		val fileStr = f.toString

		val start = fileStr.indexOf("&")
		val end = fileStr.indexOf(";")

		//print("start and end index  " + start +"\n")

		val qryStr = if (start > 0) fileStr.substring(start, end) else ""

		//print("query : " + qryStr + "\n")
		qryStr
	}

}

class BahirWebHdfsInputStream (fPath : Path, bufferSz : Int, blockSz : Long, fileSz : Long, readFull : Boolean, usrCrd : String, conns : Int, certValidation : String)
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

	var in : ByteArrayInputStream = null

	var callCount = 0

	
	override def read(b: Array[Byte], offset: Int, length: Int) : Int = {

		if (in == null) createWebHdfsInputStream(pos)

		callCount+=1

		var bCount = in.read(b, offset, length)

		//print("In read - call count : " + callCount + " , pos : " + pos + ", offset : " + offset + " , length : " + length + " , byte count total : " + bCount + "\n")

		if (bCount < 0 && pos < fileSize) 
		{ 
			//print("In read - bCount less than 0 , call count : " + callCount + " , file size : " + fileSize + " , pos : " + pos + ", offset : " + offset + " , length : " + length + " , byte count total : " + bCount + "\n")
			//createWebHdfsInputStream(pos)
			seek(pos)
			bCount = in.read(b, offset, length)
		}

		pos+=bCount

		bCount

	}

	/**
		* This is a dummy implementation as Spark does not use it. We need it here just to satisy interface contract
	*/

	override def read() =  { 
		
		read(new Array[Byte](4056), 0, 100)
	}	

	/**
		* This is a dummy implementation as Spark does not use it. We need it here just to satisy interface contract
	*/

	override def seekToNewSource(targetPos: Long) = false

	override def getPos() = {
		pos
	}


	override def seek(newPos : Long) = { 
		//print("In seek -  newpos : " + newPos + " , old pos : " + pos + "\n")
		if (pos != newPos) {
        		pos = newPos
			if (in != null) in.close
      		}	
		createWebHdfsInputStream(pos)
	}

	private def createWebHdfsInputStream(pos : Long) = { 

		val poe = if(connections == 0)  
			if(blockSize > fileSize || readFullFlg == true) fileSize else (floor(pos/blockSize).toLong + 1)*blockSize + 10000
		else
			floor(fileSize/(connections - 1)).toInt + 10000


		//print("In read - input stream null , block size : "  + blockSize +  " , file size : " + fileSize +  " , red full flg : " + readFullFlg + " , pos : " + pos + " , poe : " + poe +"\n")

		in = WebHdfsConnector.getFileInputStream(filePath.toString(), pos, poe, certValidationFlg, "1000:50000", usrCred)
	}

}
