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

package org.apache.bahir.sql.streaming.jdbc

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class JdbcSourceProvider extends StreamWriteSupport with DataSourceRegister{
  override def createStreamWriter(queryId: String, schema: StructType,
    mode: OutputMode, options: DataSourceOptions): StreamWriter = {
    val optionMap = options.asMap().asScala.toMap
    // add this for parameter check.
    new JDBCOptions(optionMap)
    new JdbcStreamWriter(schema, optionMap)
  }

  // short name 'jdbc' is used for batch, chose a different name for streaming.
  override def shortName(): String = "streaming-jdbc"
}
