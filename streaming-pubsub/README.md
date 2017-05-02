A library for reading data from [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/) using Spark Streaming.

## Linking

Using SBT:
    
    libraryDependencies += "org.apache.bahir" %% "spark-streaming-pubsub" % "2.0.0"
    
Using Maven:
    
    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-streaming-pubsub_2.11</artifactId>
        <version>2.0.0</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-streaming-pubsub_2.11:2.0.0

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

## Examples

### Scala API
    
    val lines = PubsubUtils.createStream(ssc, ...)
    
### Java API
    
    JavaDStream<SparkPubsubMessage> lines = PubsubUtils.createStream(jssc, ...) 

See end-to-end examples at [Google Cloud Pubsub Examples](streaming-pubsub/examples)
