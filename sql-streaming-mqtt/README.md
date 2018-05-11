A library for reading data from MQTT Servers using Spark SQL Streaming ( or Structured streaming.). 

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-sql-streaming-mqtt" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-sql-streaming-mqtt_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-sql-streaming-mqtt_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

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

## Configuration options.

This source uses [Eclipse Paho Java Client](https://eclipse.org/paho/clients/java/). Client API documentation is located [here](http://www.eclipse.org/paho/files/javadoc/index.html).

 * `brokerUrl` A url MqttClient connects to. Set this or `path` as the url of the Mqtt Server. e.g. tcp://localhost:1883.
 * `persistence` By default it is used for storing incoming messages on disk. If `memory` is provided as value for this option, then recovery on restart is not supported.
 * `topic` Topic MqttClient subscribes to.
 * `clientId` clientId, this client is assoicated with. Provide the same value to recover a stopped client.
 * `QoS` The maximum quality of service to subscribe each topic at. Messages published at a lower quality of service will be received at the published QoS. Messages published at a higher quality of service will be received using the QoS specified on the subscribe.
 * `username` Sets the user name to use for the connection to Mqtt Server. Do not set it, if server does not need this. Setting it empty will lead to errors.
 * `password` Sets the password to use for the connection.
 * `cleanSession` Setting it true starts a clean session, removes all checkpointed messages by a previous run of this source. This is set to false by default.
 * `connectionTimeout` Sets the connection timeout, a value of 0 is interpretted as wait until client connects. See `MqttConnectOptions.setConnectionTimeout` for more information.
 * `keepAlive` Same as `MqttConnectOptions.setKeepAliveInterval`.
 * `mqttVersion` Same as `MqttConnectOptions.setMqttVersion`.

### Scala API

An example, for scala API to count words from incoming message stream. 

    // Create DataFrame representing the stream of input lines from connection to mqtt server
    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic)
      .load(brokerUrl).selectExpr("CAST(payload AS STRING)").as[String]

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
            .load(brokerUrl)
            .selectExpr("CAST(payload AS STRING)").as(Encoders.STRING());

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

## Best Practices.

1. Turn Mqtt into a more reliable messaging service. 

> *MQTT is a machine-to-machine (M2M)/"Internet of Things" connectivity protocol. It was designed as an extremely lightweight publish/subscribe messaging transport.*

The design of Mqtt and the purpose it serves goes well together, but often in an application it is of utmost value to have reliability. Since mqtt is not a distributed message queue and thus does not offer the highest level of reliability features. It should be redirected via a kafka message queue to take advantage of a distributed message queue. In fact, using a kafka message queue offers a lot of possibilities including a single kafka topic subscribed to several mqtt sources and even a single mqtt stream publishing to multiple kafka topics. Kafka is a reliable and scalable message queue.

2. Often the message payload is not of the default character encoding or contains binary that needs to be parsed using a particular parser. In such cases, spark mqtt payload should be processed using the external parser. For example:

 * Scala API example:
```scala
    // Create DataFrame representing the stream of binary messages
    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic)
      .load(brokerUrl).select("payload").as[Array[Byte]].map(externalParser(_))
```

 * Java API example
```java
        // Create DataFrame representing the stream of binary messages
        Dataset<byte[]> lines = spark
                .readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", topic)
                .load(brokerUrl).selectExpr("CAST(payload AS BINARY)").as(Encoders.BINARY());

        // Split the lines into words
        Dataset<String> words = lines.map(new MapFunction<byte[], String>() {
            @Override
            public String call(byte[] bytes) throws Exception {
                return new String(bytes); // Plug in external parser here.
            }
        }, Encoders.STRING()).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(" ")).iterator();
            }
        }, Encoders.STRING());

```

3. What is the solution for a situation when there are a large number of varied mqtt sources, each with different schema and throughput characteristics.

This is an anti-pattern for spark structured streaming, which is designed to process a single schema, high volume streaming feed. Generally, one would create a lot of streaming pipelines to solve this problem. This would either require a very sophisticated scheduling setup or will waste a lot of resources, as it is not certain which stream is using more amount of data.

The general solution is both less optimum and is more cumbersome to operate, with multiple moving parts incurs a high maintenance overall. As an alternative, in this situation, one can setup a single topic kafka-spark stream, where message from each of the varied stream contains a unique tag separating one from other streams. This way at the processing end, one can distinguish the message from one another and apply the right kind of decoding and processing. Similarly while storing, each message can be distinguished from others by a tag that distinguishes.

