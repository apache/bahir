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

import java.sql.{Connection, PreparedStatement, SQLException}
import java.util.Locale

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.StructType

import org.apache.bahir.utils.Logging

/**
 * Dummy commit message. The DataSourceV2 framework requires a commit message implementation but we
 * don't need to really send one.
 */
case object JdbcWriterCommitMessage extends WriterCommitMessage
/**
 * A [[org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter]] for jdbc writing.
 * Responsible for generating the writer factory.
 */
class JdbcStreamWriter(
  schema: StructType,
  options: Map[String, String]
) extends StreamWriter with Logging {
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    log.info(s"epoch ${epochId} of JdbcStreamWriter commited!")
  }
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    log.info(s"epoch ${epochId} of JdbcStreamWriter aborted!")
  }

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new JdbcStreamWriterFactory(schema, options)
  }
}
/**
 * A [[DataWriterFactory]] for jdbc writing.
 * Will be serialized and sent to executors to generate the per-task data writers.
 */
case class JdbcStreamWriterFactory(
  schema: StructType,
  options: Map[String, String]
) extends DataWriterFactory[InternalRow] with Logging {
  override def createDataWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    log.info(s"Create date writer for TID ${taskId}, EpochId ${epochId}")
    JdbcStreamDataWriter(schema, options)
  }
}
/**
 * A [[org.apache.spark.sql.sources.v2.writer.DataWriter]] for Jdbc writing.
 * One data writer will be created in each partition to process incoming rows.
 */
case class JdbcStreamDataWriter(
  schema: StructType,
  options: Map[String, String]
) extends DataWriter[InternalRow] with Logging {
  private val jdbcOptions = new JDBCOptions(options)

  // use a local cache for batch write to jdbc.
  private val batchSize = jdbcOptions.batchSize
  private val localBuffer = new ArrayBuffer[Row](batchSize)
  private val maxRetryNum = options.getOrElse("maxRetryNumber", "4").toInt
  private val checkValidTimeoutSeconds =
    options.getOrElse("checkValidTimeoutSeconds", "10").toInt

  // the first part is the column name list, the second part is the placeholder string.
  private val sqlPart: (String, String) = {
    val columnListBuilder = new StringBuilder()
    val holderListBuilder = new StringBuilder()
    schema.fields.foreach { field =>
      columnListBuilder.append(",").append(field.name)
      holderListBuilder.append(",?")
    }
    (columnListBuilder.substring(1), holderListBuilder.substring(1))
  }

  private val sql = s"REPLACE INTO ${jdbcOptions.tableOrQuery} " +
    s"( ${sqlPart._1} ) values ( ${sqlPart._2} )"
  log.trace(s"Sql string for jdbc writing is ${sql}")
  private val dialect = JdbcDialects.get(jdbcOptions.url)
  // used for batch writing.
  private var conn: Connection = _
  private var stmt: PreparedStatement = _

  checkSchema()
  private val setters = schema.fields.map { f =>
    resetConnectionAndStmt()
    JdbcUtil.makeSetter(conn, dialect, f.dataType)
  }
  private val numFields = schema.fields.length
  private val nullTypes = schema.fields.map(f =>
    JdbcUtil.getJdbcType(f.dataType, dialect).jdbcNullType)
  /**
   * Check data schema with table.
   * Data schema should equal with table schema or is a subset of table schema,
   * and the column type with the same name in data schema and table scheme should be the same.
   */
  private def checkSchema(): Unit = {
    resetConnectionAndStmt()
    val tableSchemaMap = JdbcUtils.getSchemaOption(conn, jdbcOptions) match {
      case Some(tableSchema) =>
        log.info(s"Get table ${jdbcOptions.tableOrQuery}'s schema $tableSchema")
        tableSchema.fields.map(field => field.name.toLowerCase(Locale.ROOT) -> field).toMap
      case _ => throw new IllegalStateException(
        s"Schema of table ${jdbcOptions.tableOrQuery} is not defined, make sure table exist!")
    }
    schema.map { field =>
      val tableColumn = tableSchemaMap.get(field.name.toLowerCase(Locale.ROOT))
      assert(tableColumn.isDefined,
        s"Data column ${field.name} cannot be found in table ${jdbcOptions.tableOrQuery}")
      assert(field.dataType == tableColumn.get.dataType,
        s"Type of data column ${field.name} is not the same in table ${jdbcOptions.tableOrQuery}")
    }
  }
  // Using a local connection cache, avoid getting a new connection every time.
  private def resetConnectionAndStmt(): Unit = {
    if (conn == null || !conn.isValid(checkValidTimeoutSeconds)) {
      conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
      stmt = conn.prepareStatement(sql)
      log.info("Current connection is invalid, create a new one.")
    } else {
      log.debug("Current connection is valid, reuse it.")
    }
  }

  override def write(record: InternalRow): Unit = {
    localBuffer.append(Row.fromSeq(record.copy().toSeq(schema)))
    if (localBuffer.size == batchSize) {
      log.debug(s"Local buffer is full with size $batchSize, do write and reset local buffer.")
      doWriteAndResetBuffer()
    }
  }
  // batch write to jdbc, retry for SQLException
  private def doWriteAndResetBuffer(): Unit = {
    var tryNum = 0
    val size = localBuffer.size
    while (tryNum <= maxRetryNum) {
      try {
        val start = System.currentTimeMillis()
        val iterator = localBuffer.iterator
        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              setters(i).apply(stmt, row, i)
            }
            i += 1
          }
          stmt.addBatch()
        }
        stmt.executeBatch()
        localBuffer.clear()
        log.debug(s"Success write $size records,"
          + s"retry number $tryNum, cost ${System.currentTimeMillis() - start} ms")
        tryNum = maxRetryNum + 1
      } catch {
        case e: SQLException =>
          if (tryNum <= maxRetryNum) {
            tryNum += 1
            resetConnectionAndStmt()
            log.warn(s"Failed to write $size records, retry number $tryNum!", e)
          } else {
            log.error(s"Failed to write $size records,"
              + s"reach max retry number $maxRetryNum, abort writing!")
            throw e
          }
        case e: Throwable =>
          log.error(s"Failed to write $size records, not suited for retry , abort writing!", e)
          throw e
      }
    }
  }

  private def doWriteAndClose(): Unit = {
    if (localBuffer.nonEmpty) {
      doWriteAndResetBuffer()
    }
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: Throwable => log.error("Close connection with exception", e)
      }
    }
  }
  override def commit(): WriterCommitMessage = {
    doWriteAndClose()
    JdbcWriterCommitMessage
  }
  override def abort(): Unit = {
    log.info(s"Abort writing with ${localBuffer.size} records in local buffer.")
  }
}
