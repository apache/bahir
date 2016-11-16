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

package org.apache.bahir.datasource.webhdfs.csv


import java.text.SimpleDateFormat

// TODO: use scala.collection.JavaConverters instead of implicit JavaConversions
// scalastyle:off javaconversions
import scala.collection.JavaConversions._
// scalastyle:on
import scala.util.control.NonFatal

import org.apache.bahir.datasource.webhdfs.util._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * This class contains functions for  converting RDD to csv data source
 * This is copied from com.databricks.spark.csv as the required object could not be reused as it is
 * declared as private
 */
case class WebHdfsCsvRelation protected[webhdfs] (
    baseRDD: () => RDD[String],
    location: Option[String],
    useHeader: Boolean,
    delimiter: Char,
    quote: Character,
    escape: Character,
    comment: Character,
    parseMode: String,
    parserLib: String,
    ignoreLeadingWhiteSpace: Boolean,
    ignoreTrailingWhiteSpace: Boolean,
    treatEmptyValuesAsNulls: Boolean,
    userSchema: StructType = null,
    inferCsvSchema: Boolean,
    codec: String = null,
    nullValue: String = "",
    dateFormat: String = null,
    maxCharsPerCol: Int = 100000)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan {

  // Share date format object as it is expensive to parse date pattern.
  private val dateFormatter = if (dateFormat != null) new SimpleDateFormat(dateFormat) else null

  private val logger = LoggerFactory.getLogger(WebHdfsCsvRelation.getClass)

  // Parse mode flags
  if (!WebHdfsParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${WebHdfsParseModes.DEFAULT}.")
  }

  if ((ignoreLeadingWhiteSpace || ignoreLeadingWhiteSpace)
      && WebHdfsCsvParserLibs.isCommonsLib(parserLib)) {
    logger.warn(s"Ignore white space options may not work with Commons parserLib option")
  }

  private val failFast = WebHdfsParseModes.isFailFastMode(parseMode)
  private val dropMalformed = WebHdfsParseModes.isDropMalformedMode(parseMode)
  private val permissive = WebHdfsParseModes.isPermissiveMode(parseMode)

  override val schema: StructType = inferSchema()

  private def tokenRdd(header: Array[String]): RDD[Array[String]] = {

      val csvFormat = CSVFormat.DEFAULT
        .withDelimiter(delimiter)
        .withQuote(quote)
        .withEscape(escape)
        .withSkipHeaderRecord(false)
        .withHeader(header: _*)
        .withCommentMarker(comment)

      // If header is set, make sure firstLine is materialized before sending to executors.
      val filterLine = if (useHeader) firstLine else null

      baseRDD().mapPartitions { iter =>
        // When using header, any input line that equals firstLine is assumed to be header
        val csvIter = if (useHeader) {
          iter.filter(_ != filterLine)
        } else {
          iter
        }
        parseCSV(csvIter, csvFormat)
      }
  }

  override def buildScan: RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val rowArray = new Array[Any](schemaFields.length)
    tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

      if (dropMalformed && schemaFields.length != tokens.length) {
        logger.warn(s"Dropping malformed line: ${tokens.mkString(",")}")
        None
      } else if (failFast && schemaFields.length != tokens.length) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: ${tokens.mkString(",")}")
      } else {
        var index: Int = 0
        try {
          index = 0
          while (index < schemaFields.length) {
            val field = schemaFields(index)
            rowArray(index) = WebHdfsTypeCast.castTo(tokens(index), field.dataType, field.nullable,
              treatEmptyValuesAsNulls, nullValue, simpleDateFormatter)
            index = index + 1
          }
          Some(Row.fromSeq(rowArray))
        } catch {
          case aiob: ArrayIndexOutOfBoundsException if permissive =>
            (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
            Some(Row.fromSeq(rowArray))
          case _: java.lang.NumberFormatException |
               _: IllegalArgumentException if dropMalformed =>
            logger.warn("Number format exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
          case pe: java.text.ParseException if dropMalformed =>
            logger.warn("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
        }
      }
    }
  }


  /**
   * This supports to eliminate unneeded columns before producing an RDD
   * containing all of its tuples as Row objects. This reads all the tokens of each line
   * and then drop unneeded tokens without casting and type-checking by mapping
   * both the indices produced by `requiredColumns` and the ones of tokens.
   */
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields
    val shouldTableScan = schemaFields.deep == requiredFields.deep
    val safeRequiredFields = if (dropMalformed) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    } else {
      requiredFields
    }
    val rowArray = new Array[Any](safeRequiredFields.length)
    if (shouldTableScan) {
      buildScan()
    } else {
      val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
      schemaFields.zipWithIndex.filter {
        case (field, _) => safeRequiredFields.contains(field)
      }.foreach {
        case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
      }
      val requiredSize = requiredFields.length
      tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

        if (dropMalformed && schemaFields.length != tokens.length) {
          logger.warn(s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
          None
        } else if (failFast && schemaFields.length != tokens.length) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
            s"${tokens.mkString(delimiter.toString)}")
        } else {
          val indexSafeTokens = if (permissive && schemaFields.length > tokens.length) {
            tokens ++ new Array[String](schemaFields.length - tokens.length)
          } else if (permissive && schemaFields.length < tokens.length) {
            tokens.take(schemaFields.length)
          } else {
            tokens
          }
          try {
            var index: Int = 0
            var subIndex: Int = 0
            while (subIndex < safeRequiredIndices.length) {
              index = safeRequiredIndices(subIndex)
              val field = schemaFields(index)
              rowArray(subIndex) = WebHdfsTypeCast.castTo(
                indexSafeTokens(index),
                field.dataType,
                field.nullable,
                treatEmptyValuesAsNulls,
                nullValue,
                simpleDateFormatter
              )
              subIndex = subIndex + 1
            }
            Some(Row.fromSeq(rowArray.take(requiredSize)))
          } catch {
            case _: java.lang.NumberFormatException |
                 _: IllegalArgumentException if dropMalformed =>
              logger.warn("Number format exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            case pe: java.text.ParseException if dropMalformed =>
              logger.warn("Parse exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
          }
        }
      }
    }
  }

  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
        val csvFormat = CSVFormat.DEFAULT
          .withDelimiter(delimiter)
          .withQuote(quote)
          .withEscape(escape)
          .withSkipHeaderRecord(false)
        val firstRow = CSVParser.parse(firstLine, csvFormat).getRecords.head.toArray
      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index"}
      }
      if (this.inferCsvSchema) {
        val simpleDateFormatter = dateFormatter
        WebHdfsInferSchema(tokenRdd(header), header, nullValue, simpleDateFormatter)
      } else {
        // By default fields are assumed to be StringType
        val schemaFields = header.map { fieldName =>
          StructField(fieldName.toString, StringType, nullable = true)
        }
        StructType(schemaFields)
      }
    }
  }

  /**
   * Returns the first line of the first non-empty file in path
   */
  private lazy val firstLine = {
    if (comment != null) {
      baseRDD().filter { line =>
        line.trim.nonEmpty && !line.startsWith(comment.toString)
      }.first()
    } else {
      baseRDD().filter { line =>
        line.trim.nonEmpty
      }.first()
    }
  }


  private def parseCSV(
      iter: Iterator[String],
      csvFormat: CSVFormat): Iterator[Array[String]] = {
    iter.flatMap { line =>
      try {
        val records = CSVParser.parse(line, csvFormat).getRecords
        if (records.isEmpty) {
          logger.warn(s"Ignoring empty line: $line")
          None
        } else {
          Some(records.head.toArray)
        }
      } catch {
        case NonFatal(e) if !failFast =>
          logger.error(s"Exception while parsing line: $line. ", e)
          None
      }
    }
  }

}
