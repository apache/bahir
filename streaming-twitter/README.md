
A library for reading social data from [twitter](http://twitter.com/) using Spark Streaming. 

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-streaming-twitter_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-streaming-twitter_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is cross-published for Scala 2.10 and Scala 2.11, so users should replace the proper Scala version (2.10 or 2.11) in the commands listed above.


## Examples

`TwitterUtils` uses Twitter4j to get the public stream of tweets using [Twitter's Streaming API](https://dev.twitter.com/docs/streaming-apis). Authentication information
can be provided by any of the [methods](http://twitter4j.org/en/configuration.html) supported by Twitter4J library. You can import the `TwitterUtils` class and create a DStream with `TwitterUtils.createStream` as shown below.

### Scala API

    import org.apache.spark.streaming.twitter._

    TwitterUtils.createStream(ssc, None)

### Java API

    import org.apache.spark.streaming.twitter.*;

    TwitterUtils.createStream(jssc);


You can also either get the public stream, or get the filtered stream based on keywords. 
See end-to-end examples at [Twitter Examples](https://github.com/apache/bahir/tree/master/streaming-twitter/examples)