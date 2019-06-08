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

package org.apache.bahir.sql.streaming.mqtt

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.security.Groups

object HDFSTestUtils {
  def prepareHadoop(): (Configuration, MiniDFSCluster) = {
    val baseDir = new File(System.getProperty("java.io.tmpdir") + "/hadoop").getAbsoluteFile
    System.setProperty("HADOOP_USER_NAME", "test")
    val config = new Configuration
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    config.set("dfs.namenode.acls.enabled", "true")
    config.set("dfs.permissions", "true")
    Groups.getUserToGroupsMappingService(config)
    val builder = new MiniDFSCluster.Builder(config)
    val hadoop = builder.build
    config.set("fs.defaultFS", "hdfs://localhost:" + hadoop.getNameNodePort + "/")
    (config, hadoop)
  }
}
