
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


### Scala API

    val lines = ZeroMQUtils.createStream(ssc, ...)

### Java API

    JavaDStream<String> lines = ZeroMQUtils.createStream(jssc, ...);

See end-to-end examples at [ZeroMQ Examples](https://github.com/apache/bahir/tree/master/streaming-zeromq/examples)