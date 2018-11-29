# Spark Streaming ZeroMQ Connector

A library for reading data from [ZeroMQ](http://zeromq.org/) using Spark Streaming. 

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-streaming-zeromq" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-streaming-zeromq_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-streaming-zeromq_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is cross-published for Scala 2.10 and Scala 2.11, so users should replace the proper Scala version (2.10 or 2.11) in the commands listed above.

## Examples

Review end-to-end examples at [ZeroMQ Examples](https://github.com/apache/bahir/tree/master/streaming-zeromq/examples).

### Scala API

    import org.apache.spark.streaming.zeromq.ZeroMQUtils

    val lines = ZeroMQUtils.createTextStream(
      ssc, "tcp://server:5555", true, Seq("my-topic".getBytes)
    )

### Java API

    import org.apache.spark.storage.StorageLevel;
    import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
    import org.apache.spark.streaming.zeromq.ZeroMQUtils;

    JavaReceiverInputDStream<String> test1 = ZeroMQUtils.createJavaStream(
        ssc, "tcp://server:5555", true, Arrays.asList("my-topic.getBytes()),
        StorageLevel.MEMORY_AND_DISK_SER_2()
    );