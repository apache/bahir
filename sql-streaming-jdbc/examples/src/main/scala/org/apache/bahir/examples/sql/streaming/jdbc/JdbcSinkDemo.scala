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

package org.apache.bahir.examples.sql.streaming.jdbc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * Mock using rate source, change the log to a simple Person
 * object with name and age property, and write to jdbc.
 *
 * Usage: JdbcSinkDemo <jdbcUrl> <tableName> <username> <password>
 */
object JdbcSinkDemo {

  private case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      // scalastyle:off println
      System.err.println("Usage: JdbcSinkDemo <jdbcUrl> <tableName> <username> <password>")
      // scalastyle:on
      System.exit(1)
    }

    val jdbcUrl = args(0)
    val tableName = args(1)
    val username = args(2)
    val password = args(3)

    val spark = SparkSession
      .builder()
      .appName("JdbcSinkDemo")
      .getOrCreate()

    // load data source
    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "5")
      .option("rowsPerSecond", "100")
      .load()

    // change input value to a person object.
    import spark.implicits._
    val lines = df.select("value").as[Long].map{ value =>
      Person(s"name_${value}", value.toInt % 30)
    }

    lines.printSchema()

    // write result
    val query = lines.writeStream
      .outputMode("append")
      .format("streaming-jdbc")
      .outputMode(OutputMode.Append)
      .option(JDBCOptions.JDBC_URL, jdbcUrl)
      .option(JDBCOptions.JDBC_TABLE_NAME, tableName)
      .option(JDBCOptions.JDBC_DRIVER_CLASS, "com.mysql.jdbc.Driver")
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, "5")
      .option("user", username)
      .option("password", password)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
