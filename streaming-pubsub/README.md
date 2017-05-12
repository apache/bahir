A library for reading data from [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/) using Spark Streaming.

## Linking

Using SBT:
    
    libraryDependencies += "org.apache.bahir" %% "spark-streaming-pubsub" % "2.0.0"
    
Using Maven:
    
    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-streaming-pubsub_2.11</artifactId>
        <version>2.1.0</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-streaming-pubsub_2.11:2.0.0

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

## Examples

First you need to create credential by SparkGCPCredentials, it support four type of credentials
* application default
    `SparkGCPCredentials.builder.build()`
* json type service account
    `SparkGCPCredentials.builder.jsonServiceAccount(PATH_TO_JSON_KEY).build()`
* p12 type service account
    `SparkGCPCredentials.builder.p12ServiceAccount(PATH_TO_P12_KEY, EMAIL_ACCOUNT).build()`
* metadata service account(running on dataproc)
    `SparkGCPCredentials.builder.metadataServiceAccount().build()`

### Scala API
    
    val lines = PubsubUtils.createStream(ssc, projectId, subscriptionName, credential, ..)
    
### Java API
    
    JavaDStream<SparkPubsubMessage> lines = PubsubUtils.createStream(jssc, projectId, subscriptionName, credential...) 

See end-to-end examples at [Google Cloud Pubsub Examples](streaming-pubsub/examples)
