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

package org.apache.bahir.examples.sql.streaming.mqtt;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from MQTT Server.
 *
 * Usage: JavaMQTTStreamWordCount <brokerUrl> <topic>
 * <brokerUrl> and <topic> describe the MQTT server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, a MQTT Server should be up and running.
 *
 */
public final class JavaMQTTStreamWordCount {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaMQTTStreamWordCount <brokerUrl> <topic>");
            System.exit(1);
        }

        if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            Logger.getRootLogger().setLevel(Level.WARN);
        }

        String brokerUrl = args[0];
        String topic = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("JavaMQTTStreamWordCount");

        // check Spark configuration for master URL, set it to local if not configured
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[4]");
        }

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to mqtt server
        Dataset<String> lines = spark
                .readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", topic)
                .load(brokerUrl).select("value").as(Encoders.STRING());

        // Split the lines into words
        Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(" ")).iterator();
            }
        }, Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
