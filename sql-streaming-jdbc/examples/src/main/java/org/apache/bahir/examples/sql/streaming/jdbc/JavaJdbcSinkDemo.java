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

package org.apache.bahir.examples.sql.streaming.jdbc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Mock using rate source, change the log to a simple Person
 * object with name and age property, and write to jdbc.
 *
 * Usage: JdbcSinkDemo <jdbcUrl> <tableName> <username> <password>
 */
public class JavaJdbcSinkDemo {

    public static void main(String[] args) throws Exception{
        if (args.length < 4) {
            System.err.println("Usage: JdbcSinkDemo <jdbcUrl> <tableName> <username> <password>");
            System.exit(1);
        }

        String jdbcUrl = args[0];
        String tableName = args[1];
        String username = args[2];
        String password = args[3];

        SparkConf sparkConf = new SparkConf().setAppName("JavaJdbcSinkDemo");

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // load data source
        Dataset<Long> lines = spark
                .readStream()
                .format("rate")
                .option("numPartitions", "5")
                .option("rowsPerSecond", "100")
                .load().select("value").as(Encoders.LONG());
        // change input value to a person object.
        DemoMapFunction demoFunction = new DemoMapFunction();
        Dataset<Person> result = lines.map(demoFunction, Encoders.javaSerialization(Person.class));

        // print schema for debug
        result.printSchema();

        StreamingQuery query = result
                .writeStream()
                .outputMode("append")
                .format("streaming-jdbc")
                .outputMode(OutputMode.Append())
                .option(JDBCOptions.JDBC_URL(), jdbcUrl)
                .option(JDBCOptions.JDBC_TABLE_NAME(), tableName)
                .option(JDBCOptions.JDBC_DRIVER_CLASS(), "com.mysql.jdbc.Driver")
                .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), "5")
                .option("user", username)
                .option("password", password)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();
        query.awaitTermination();

    }

    private static class Person {
        private String name;
        private int age;

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    private static class DemoMapFunction implements MapFunction<Long, Person> {

        @Override
        public Person call(Long value) throws Exception {
            return new Person("name_" + value, value.intValue() % 30);
        }
    }
}
