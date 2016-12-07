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

package org.apache.hadoop.hdfs.web

import java.net.URL
import java.net.URI

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.web.resources.HttpOpParam.Op
import org.apache.hadoop.hdfs.web.resources.Param

import org.apache.hadoop.conf.Configuration

/**
 * A FileSystem for HDFS over the web, extending [[org.apache.hadoop.hdfs.web.WebHdfsFileSystem]]
 * to allow secure authentication and configurable gateway path segments.
 *
 * TODO: upgrade Hadoop version (Maven dependencies) to override SWebHdfsFileSystem
 */
class BahirWebHdfsFileSystem extends WebHdfsFileSystem {

  // TODO: figure out how/where to authenticate

  // TODO: gateway path should be configurable
  val gatewayPath = "/gateway/default"

 	var uri:URI = null
 	var rHdfsUri:URI = null
 	var conf:Configuration = null

	override def initialize(
      		uriOrg: URI,
      		confOrg: Configuration): Unit = {
	
		super.initialize(uriOrg, confOrg)

		uri = URI.create(uriOrg.getScheme() + "://" + uriOrg.getAuthority()+"/gateway/default")	
		print("uri in initialize : " +  uri + "\n")

  	}

  	//override def toUrl(op: HttpOpParam.Op, fspath: Path, parameters: Param[_, _]*): URL = {
  	override def toUrl(op: Op, fspath: Path, parameters: Param[_, _]*): URL = {
    		var url = super.toUrl(op, fspath, parameters: _*)
    		url = new URL("http://www.google.com")

		print("url in overriden toUrl: " + url + "\n")

    		new URL("https", url.getHost, url.getPort,
      			url.getFile.replaceFirst(WebHdfsFileSystem.PATH_PREFIX,
        		gatewayPath + WebHdfsFileSystem.PATH_PREFIX))
  	}

}
