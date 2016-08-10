A library for reading data from MQTT Servers using Spark SQL Streaming ( or Structured streaming.). 

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-sql-streaming-mqtt" % "2.0.0"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-sql-streaming-mqtt_2.11</artifactId>
        <version>2.0.0</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-sql-streaming-mqtt_2.11:2.0.0

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is compiled for Scala 2.11 only, and intends to support Spark 2.0 onwards.

## Examples

A SQL Stream can be created with data streams received through MQTT Server using,

    sqlContext.readStream
        .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
        .option("topic", "mytopic")
        .load("tcp://localhost:1883")

## Enable recovering from failures.

Setting values for option `localStorage` and `clientId` helps in recovering in case of a restart, by restoring the state where it left off before the shutdown.

    sqlContext.readStream
        .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
        .option("topic", "mytopic")
        .option("localStorage", "/path/to/localdir")
        .option("clientId", "some-client-id")
        .load("tcp://localhost:1883")

### Scala API

An example, for scala API to count words from incoming message stream. 

    // Create DataFrame representing the stream of input lines from connection to mqtt server
    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic)
      .load(brokerUrl).as[(String, Timestamp)]

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

Please see `MQTTStreamWordCount.scala` for full example.

### Java API

An example, for Java API to count words from incoming message stream. 

    // Create DataFrame representing the stream of input lines from connection to mqtt server.
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

Please see `JavaMQTTStreamWordCount.java` for full example.

