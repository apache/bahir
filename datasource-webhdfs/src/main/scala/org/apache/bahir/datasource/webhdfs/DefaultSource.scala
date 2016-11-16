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

import scala.annotation.switch

import org.apache.bahir.datasource.webhdfs.csv._
import org.apache.bahir.datasource.webhdfs.util._

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}


/**
 * A Spark data source to read/write data from/to remote WebHDFS servers.
 * This function is written in line with the DataSource function in com.databricks.spark.csv
 */
class DefaultSource
  extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider
    with DataSourceRegister {

  override def shortName() : String = "webhdfs"

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified "))
  }

  /**
   * Creates a new relation for data store in CSV given parameters.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in CSV given parameters and user supported schema.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): WebHdfsCsvRelation = {
    // print("In Create Relation of DefaultSource" + "\n")
    val path = checkPath(parameters)
    val delimiter = WebHdfsTypeCast.toChar(parameters.getOrElse("delimiter", ","))

    val quote = parameters.getOrElse("quote", "\"")
    val quoteChar: Character = if (quote == null) {
      null
    } else if (quote.length == 1) {
      quote.charAt(0)
    } else {
      throw new Exception("Quotation cannot be more than one character.")
    }

    val escape = parameters.getOrElse("escape", null)
    val escapeChar: Character = if (escape == null) {
      null
    } else if (escape.length == 1) {
      escape.charAt(0)
    } else {
      throw new Exception("Escape character cannot be more than one character.")
    }

    val comment = parameters.getOrElse("comment", "#")
    val commentChar: Character = if (comment == null) {
      null
    } else if (comment.length == 1) {
      comment.charAt(0)
    } else {
      throw new Exception("Comment marker cannot be more than one character.")
    }

    val parseMode = parameters.getOrElse("mode", "PERMISSIVE")

    val useHeader = parameters.getOrElse("header", "false")
    val headerFlag = if (useHeader == "true") {
      true
    } else if (useHeader == "false") {
      false
    } else {
      throw new Exception("Header flag must be true or false")
    }

    // val parserLib = parameters.getOrElse("parserLib", ParserLibs.DEFAULT)
    val parserLib = parameters.getOrElse("parserLib", "COMMONS")
    val ignoreLeadingWhiteSpace = parameters.getOrElse("ignoreLeadingWhiteSpace", "false")
    val ignoreLeadingWhiteSpaceFlag = if (ignoreLeadingWhiteSpace == "false") {
      false
    } else if (ignoreLeadingWhiteSpace == "true") {
      // if (!ParserLibs.isUnivocityLib(parserLib)) {
      //   throw new Exception("Ignore white space supported for Univocity parser only")
      // }
      true
    } else {
      throw new Exception("Ignore white space flag must be true or false")
    }

    val ignoreTrailingWhiteSpace = parameters.getOrElse("ignoreTrailingWhiteSpace", "false")
    val ignoreTrailingWhiteSpaceFlag = if (ignoreTrailingWhiteSpace == "false") {
      false
    } else if (ignoreTrailingWhiteSpace == "true") {
      // if (!ParserLibs.isUnivocityLib(parserLib)) {
      //   throw new Exception("Ignore white space supported for the Univocity parser only")
      // }
      true
    } else {
      throw new Exception("Ignore white space flag can be true or false")
    }
    val treatEmptyValuesAsNulls = parameters.getOrElse("treatEmptyValuesAsNulls", "false")
    val treatEmptyValuesAsNullsFlag = if (treatEmptyValuesAsNulls == "false") {
      false
    } else if (treatEmptyValuesAsNulls == "true") {
      true
    } else {
      throw new Exception("Treat empty values as null flag can be true or false")
    }

    val charset = parameters.getOrElse("charset", WebHdfsConnector.DEFAULT_CHARSET.name())
    // TODO validate charset?

    val inferSchema = parameters.getOrElse("inferSchema", "false")
    val inferSchemaFlag = if (inferSchema == "false") {
      false
    } else if (inferSchema == "true") {
      true
    } else {
      throw new Exception("Infer schema flag can be true or false")
    }
    val nullValue = parameters.getOrElse("nullValue", "")
    val dateFormat = parameters.getOrElse("dateFormat", null)
    val codec = parameters.getOrElse("codec", null)
    val maxCharsPerColStr = parameters.getOrElse("maxCharsPerCol", "100000")
    val maxCharsPerCol = try {
      maxCharsPerColStr.toInt
    } catch {
      case e: Exception => throw new Exception("maxCharsPerCol must be a valid integer")
    }

    val trustCredStr = parameters.getOrElse("certValidation", "")
    val userCredStr = parameters.getOrElse("userCred", "")
    if (userCredStr == "") {
      throw new Exception("User Credential has to  be set")
    }

    val connPropStr = parameters.getOrElse("connProp", "10000:60000")
    var partitionDetailsStr = parameters.getOrElse("partitions", "4:10000")

    if (!partitionDetailsStr.contains(":")) partitionDetailsStr = partitionDetailsStr + ":10000"

    val formatDetailsStr = parameters.getOrElse("format", "csv:\n")
    val formatDetailsArr = formatDetailsStr.split(":")
    var formatRecordSeparator = formatDetailsArr(1)
    val outputTypeStr = parameters.getOrElse("output", "Data")
    val formatType = formatDetailsArr(0)
    if (formatType != "csv" && formatRecordSeparator == "") {
      throw new Exception("Record Separator cannot be inferred for Format other than csv")
    }
    if (formatType == "csv" && formatRecordSeparator == "") formatRecordSeparator = "\n"

    val outRdd = (outputTypeStr : @switch) match {
      case "LIST" => WebHdfsConnector.listFromWebHdfs(sqlContext.sparkContext, path, trustCredStr,
        userCredStr, connPropStr)
      case default => WebHdfsConnector.loadFromWebHdfs(sqlContext.sparkContext, path, charset,
        trustCredStr, userCredStr, connPropStr, partitionDetailsStr, formatRecordSeparator)
    }

    val targetSchema = (outputTypeStr : @switch) match {
      case "LIST" => StructType(Array(
        StructField("Name", StringType, true),
        StructField("File Size", LongType, true),
        StructField("Block Size", LongType, true),
        StructField("# of Blocks", IntegerType, true)))
      case default => schema
    }

    val relation = (formatType : @switch) match {
      case "csv" =>
        WebHdfsCsvRelation(
          () => outRdd,
          Some(path),
          headerFlag,
          delimiter,
          quoteChar,
          escapeChar,
          commentChar,
          parseMode,
          parserLib,
          ignoreLeadingWhiteSpaceFlag,
          ignoreTrailingWhiteSpaceFlag,
          treatEmptyValuesAsNullsFlag,
          targetSchema,
          inferSchemaFlag,
          codec,
          nullValue,
          dateFormat,
          maxCharsPerCol)(sqlContext)
      case default => throw new Exception("Format Not Supported")
    }

    relation
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val trustStoreCredStr = parameters.getOrElse("certValidation", "")
    val connStr = parameters.getOrElse("connProp", "1000:10000")
    val partitionStr = parameters.getOrElse("partitions", "4")
    val formatStr = parameters.getOrElse("format", "csv")
    val userCredStr = parameters.getOrElse("userCred", "")
    if (userCredStr == "") {
      throw new Exception("User Credentials cannot be null")
    }

    // As of now only CSV format is supported
    val rddToWrite = (formatStr : @switch) match {
      case "csv" => WebHdfsCsvFormatter.convToCsvFormat(data, parameters)
      case default => throw new Exception("Format Not Supported")
    }

    WebHdfsConnector
      .writeToWebHdfs(rddToWrite, path, trustStoreCredStr, connStr, userCredStr, partitionStr)

    createRelation(sqlContext, parameters, data.schema)
  }
}
