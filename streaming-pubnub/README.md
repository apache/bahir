<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# Spark Streaming PubNub Connector

Library for reading data from real-time messaging infrastructure [PubNub](https://www.pubnub.com/) using Spark Streaming.

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-streaming-pubnub" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-streaming-pubnub_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-streaming-pubnub_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

## Examples

Connector leverages official Java client for PubNub cloud infrastructure. You can import the `PubNubUtils`
class and create input stream by calling `PubNubUtils.createStream()` as shown below. Security and performance related
features shall be setup inside standard `PNConfiguration` object. We advise to configure reconnection policy so that
temporary network outages do not interrupt processing job. Users may subscribe to multiple channels and channel groups,
as well as specify time token to start receiving messages since given point in time.

For complete code examples, please review _examples_ directory.

### Scala API

    import com.pubnub.api.PNConfiguration
    import com.pubnub.api.enums.PNReconnectionPolicy

    import org.apache.spark.streaming.pubnub.{PubNubUtils, SparkPubNubMessage}

    val config = new PNConfiguration
    config.setSubscribeKey(subscribeKey)
    config.setSecure(true)
    config.setReconnectionPolicy(PNReconnectionPolicy.LINEAR)
    val channel = "my-channel"

    val pubNubStream: ReceiverInputDStream[SparkPubNubMessage] = PubNubUtils.createStream(
      ssc, config, Seq(channel), Seq(), None, StorageLevel.MEMORY_AND_DISK_SER_2
    )

### Java API

    import com.pubnub.api.PNConfiguration
    import com.pubnub.api.enums.PNReconnectionPolicy

    import org.apache.spark.streaming.pubnub.PubNubUtils
    import org.apache.spark.streaming.pubnub.SparkPubNubMessage

    PNConfiguration config = new PNConfiguration()
    config.setSubscribeKey(subscribeKey)
    config.setSecure(true)
    config.setReconnectionPolicy(PNReconnectionPolicy.LINEAR)
    Set<String> channels = new HashSet<String>() {{
        add("my-channel");
    }};

    ReceiverInputDStream<SparkPubNubMessage> pubNubStream = PubNubUtils.createStream(
      ssc, config, channels, Collections.EMPTY_SET, null,
      StorageLevel.MEMORY_AND_DISK_SER_2()
    )

## Unit Test

Unit tests take advantage of publicly available _demo_ subscription and publish key, which have limited request rate.
Anyone playing with PubNub _demo_ credentials may interrupt the tests, therefore execution of integration tests
has to be explicitly enabled by setting environment variable _ENABLE_PUBNUB_TESTS_ to _1_.

    cd streaming-pubnub
    ENABLE_PUBNUB_TESTS=1 mvn clean test
