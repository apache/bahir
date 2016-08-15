
[MQTT](http://mqtt.org/) is MQTT is a machine-to-machine (M2M)/"Internet of Things" connectivity protocol. It was designed as an extremely lightweight publish/subscribe messaging transport. It is useful for connections with remote locations where a small code footprint is required and/or network bandwidth is at a premium. 

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-streaming-mqtt" % "2.1.0-SNAPSHOT"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-streaming-mqtt_2.11</artifactId>
        <version>2.1.0-SNAPSHOT</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-streaming_mqtt_2.11:2.1.0-SNAPSHOT

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is cross-published for Scala 2.10 and Scala 2.11, so users should replace the proper Scala version (2.10 or 2.11) in the commands listed above.

## Examples

### Scala API

You need to extend `ActorReceiver` so as to store received data into Spark using `store(...)` methods. The supervisor strategy of
this actor can be configured to handle failures, etc.

    val lines = MQTTUtils.createStream(ssc, brokerUrl, topic)

### Java API

You need to extend `JavaActorReceiver` so as to store received data into Spark using `store(...)` methods. The supervisor strategy of
this actor can be configured to handle failures, etc.

    JavaDStream<String> lines = MQTTUtils.createStream(jssc, brokerUrl, topic);

See end-to-end examples at [MQTT Examples](https://github.com/apache/bahir/tree/master/streaming-mqtt/examples)