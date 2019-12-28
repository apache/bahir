<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# Spark SQL Streaming Akka Data Source

A library for reading data from Akka Actors using Spark SQL Streaming ( or Structured streaming.).

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-sql-streaming-akka" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-sql-streaming-akka_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-sql-streaming-akka_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is cross-published for Scala 2.11 and Scala 2.12, so users should replace the proper Scala version in the commands listed above.

## Examples

A SQL Stream can be created with data streams received from Akka Feeder actor using,

        sqlContext.readStream
                .format("org.apache.bahir.sql.streaming.akka.AkkaStreamSourceProvider")
                .option("urlOfPublisher", "feederActorUri")
                .load()

## Enable recovering from failures.

Setting values for option `persistenceDirPath` helps in recovering in case of a restart, by restoring the state where it left off before the shutdown.

        sqlContext.readStream
                .format("org.apache.bahir.sql.streaming.akka.AkkaStreamSourceProvider")
                .option("urlOfPublisher", "feederActorUri")
                .option("persistenceDirPath", "/path/to/localdir")
                .load()

## Configuration options.

This source uses [Akka Actor api](http://doc.akka.io/api/akka/2.5/akka/actor/Actor.html).

* `urlOfPublisher` The url of Publisher or Feeder actor that the Receiver actor connects to. Set this as the tcp url of the Publisher or Feeder actor.
* `persistenceDirPath` By default it is used for storing incoming messages on disk.

### Scala API

An example, for scala API to count words from incoming message stream.

        // Create DataFrame representing the stream of input lines from connection
        // to publisher or feeder actor
        val lines = spark.readStream
                    .format("org.apache.bahir.sql.streaming.akka.AkkaStreamSourceProvider")
                    .option("urlOfPublisher", urlOfPublisher)
                    .load().as[(String, Timestamp)]

        // Split the lines into words
        val words = lines.map(_._1).flatMap(_.split(" "))

        // Generate running word count
        val wordCounts = words.groupBy("value").count()

        // Start running the query that prints the running counts to the console
        val query = wordCounts.writeStream
                    .outputMode("complete")
                    .format("console")
                    .start()

        query.awaitTermination()

Please see `AkkaStreamWordCount.scala` for full example.     

### Java API

An example, for Java API to count words from incoming message stream.

        // Create DataFrame representing the stream of input lines from connection
        // to publisher or feeder actor
        Dataset<String> lines = spark
                                .readStream()
                                .format("org.apache.bahir.sql.streaming.akka.AkkaStreamSourceProvider")
                                .option("urlOfPublisher", urlOfPublisher)
                                .load().select("value").as(Encoders.STRING());

        // Split the lines into words
        Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
          @Override
          public Iterator<String> call(String s) throws Exception {
            return Arrays.asList(s.split(" ")).iterator();
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

Please see `JavaAkkaStreamWordCount.java` for full example.      
