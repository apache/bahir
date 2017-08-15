
A library for reading data from Akka Actors using Spark Streaming. 

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-streaming-akka" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-streaming-akka_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-streaming-akka_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is cross-published for Scala 2.10 and Scala 2.11, so users should replace the proper Scala version (2.10 or 2.11) in the commands listed above.

## Examples

DStreams can be created with data streams received through Akka actors by using `AkkaUtils.createStream(ssc, actorProps, actor-name)`.

### Scala API

You need to extend `ActorReceiver` so as to store received data into Spark using `store(...)` methods. The supervisor strategy of
this actor can be configured to handle failures, etc.

    class CustomActor extends ActorReceiver {
      def receive = {
        case data: String => store(data)
      }
    }

    // A new input stream can be created with this custom actor as
    val ssc: StreamingContext = ...
    val lines = AkkaUtils.createStream[String](ssc, Props[CustomActor](), "CustomReceiver")

### Java API

You need to extend `JavaActorReceiver` so as to store received data into Spark using `store(...)` methods. The supervisor strategy of
this actor can be configured to handle failures, etc.

    class CustomActor extends JavaActorReceiver {
        @Override
        public void onReceive(Object msg) throws Exception {
            store((String) msg);
        }
    }

    // A new input stream can be created with this custom actor as
    JavaStreamingContext jssc = ...;
    JavaDStream<String> lines = AkkaUtils.<String>createStream(jssc, Props.create(CustomActor.class), "CustomReceiver");

See end-to-end examples at [Akka Examples](https://github.com/apache/bahir/tree/master/streaming-akka/examples)
