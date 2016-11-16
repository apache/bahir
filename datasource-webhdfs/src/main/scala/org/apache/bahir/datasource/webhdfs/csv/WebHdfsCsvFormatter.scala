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

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.commons.csv.{CSVFormat, QuoteMode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._


/**
 * This object contains all utility functions for converting data to CSV format
 * This is copied from com.databricks.spark.csv as the required object could not be reused as it is
 * declared as private
 */
private[webhdfs] object WebHdfsCsvFormatter {

  def convToCsvFormat(dataFrame: DataFrame,
                      parameters: Map[String, String] = Map()) : RDD[String] = {
    val delimiter = parameters.getOrElse("delimiter", ",")
    val dateFormat = parameters.getOrElse("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    val dateFormatter: SimpleDateFormat = new SimpleDateFormat(dateFormat)

    val delimiterChar = if (delimiter.length == 1) {
      delimiter.charAt(0)
    } else {
      throw new Exception("Delimiter cannot be more than one character.")
    }

    val escape = parameters.getOrElse("escape", null)
    val escapeChar: Character = if (escape == null) {
      null
    } else if (escape.length == 1) {
      escape.charAt(0)
    } else {
      throw new Exception("Escape character cannot be more than one character.")
    }

    val quote = parameters.getOrElse("quote", "\"")
    val quoteChar: Character = if (quote == null) {
      null
    } else if (quote.length == 1) {
      quote.charAt(0)
    } else {
      throw new Exception("Quotation cannot be more than one character.")
    }

    val quoteModeString = parameters.getOrElse("quoteMode", "MINIMAL")
    val quoteMode: QuoteMode = if (quoteModeString == null) {
      null
    } else {
      QuoteMode.valueOf(quoteModeString.toUpperCase)
    }

    val nullValue = parameters.getOrElse("nullValue", "null")

    val csvFormat = CSVFormat.DEFAULT
      .withDelimiter(delimiterChar)
      .withQuote(quoteChar)
      .withEscape(escapeChar)
      .withQuoteMode(quoteMode)
      .withSkipHeaderRecord(false)
      .withNullString(nullValue)

    val generateHeader = parameters.getOrElse("header", "false").toBoolean
    val header = if (generateHeader) {
      csvFormat.format(dataFrame.columns.map(_.asInstanceOf[AnyRef]): _*)
    } else {
      "" // There is no need to generate header in this case
    }

    val schema = dataFrame.schema
    val formatForIdx = schema.fieldNames.map(fname => schema(fname).dataType match {
      case TimestampType => (timestamp: Any) => {
        if (timestamp == null) {
          nullValue
        } else {
          dateFormatter.format(new java.sql.Date(timestamp.asInstanceOf[Timestamp].getTime))
        }
      }
      case DateType => (date: Any) => {
        if (date == null) nullValue else dateFormatter.format(date)
      }
      case _ => (fieldValue: Any) => fieldValue.asInstanceOf[AnyRef]
    })

    val strRDD = dataFrame.rdd.mapPartitionsWithIndex { case (index, iter) =>
      val csvFormat = CSVFormat.DEFAULT
        .withDelimiter(delimiterChar)
        .withQuote(quoteChar)
        .withEscape(escapeChar)
        .withQuoteMode(quoteMode)
        .withSkipHeaderRecord(false)
        .withNullString(nullValue)

      new Iterator[String] {
        var firstRow: Boolean = generateHeader

        override def hasNext: Boolean = iter.hasNext || firstRow

        override def next: String = {
          if (iter.nonEmpty) {
            // try .zipWithIndex.foreach
            val values: Seq[AnyRef] = iter.next().toSeq.zipWithIndex.map {
              case (fieldVal, i) => formatForIdx(i)(fieldVal)
            }
            val row = csvFormat.format(values: _*)
            if (firstRow) {
              firstRow = false
              header +  "\n" + row
            } else {
              row
            }
          } else {
            firstRow = false
            header
          }
        }
      }
    }
    strRDD
  }
}
