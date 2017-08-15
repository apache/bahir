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

This library is compiled for Scala 2.11 only, and intends to support Spark 2.0 onwards.

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
                       
This source uses [Akka Actor api](http://doc.akka.io/api/akka/2.4/akka/actor/Actor.html).
                       
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
