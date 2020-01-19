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
# Apache Bahir

Apache Bahir provides extensions to distributed analytics platforms such as Apache Spark & Apache Flink.

<http://bahir.apache.org/>

## Apache Bahir origins

The Initial Bahir source code (see issue [BAHIR-1](https://issues.apache.org/jira/browse/BAHIR-1)) containing the source for the Apache Spark streaming connectors for akka, mqtt, twitter, zeromq
extracted from [Apache Spark revision 8301fad](https://github.com/apache/spark/tree/8301fadd8d269da11e72870b7a889596e3337839)
(before the [deletion of the streaming connectors akka, mqtt, twitter, zeromq](https://issues.apache.org/jira/browse/SPARK-13843)).

## Source code structure

Source code folder structure:
```
- streaming-akka
  - examples/src/main/...
  - src/main/...
- streaming-mqtt
  - examples
  - src
  - python
- ...
```

## Building Bahir

Bahir is built using [Apache Maven](http://maven.apache.org/).
To build Bahir and its example programs, run:

    mvn -DskipTests clean install

## Running tests

Testing first requires [building Bahir](#building-bahir). Once Bahir is built, tests
can be run using:

    mvn test

## Example programs

Each extension currently available in Apache Bahir has an example application located under the "examples" folder.


## Documentation

Currently, each submodule has its own README.md, with information on example usages and API.

* [SQL Cloudant](https://github.com/apache/bahir/blob/master/sql-cloudant/README.md)
* [SQL Streaming Akka](https://github.com/apache/bahir/blob/master/sql-streaming-akka/README.md)
* [SQL Streaming JDBC](https://github.com/apache/bahir/blob/master/sql-streaming-jdbc/README.md)
* [SQL Streaming MQTT](https://github.com/apache/bahir/blob/master/sql-streaming-mqtt/README.md)
* [SQL Streaming SQS](https://github.com/apache/bahir/blob/master/sql-streaming-sqs/README.md)
* [Streaming Akka](https://github.com/apache/bahir/blob/master/streaming-akka/README.md)
* [Streaming MQTT](https://github.com/apache/bahir/blob/master/streaming-mqtt/README.md)
* [Streaming PubNub](https://github.com/apache/bahir/blob/master/streaming-pubnub/README.md)
* [Streaming Google Pub/Sub](https://github.com/apache/bahir/blob/master/streaming-pubsub/README.md)
* [Streaming Twitter](https://github.com/apache/bahir/blob/master/streaming-twitter/README.md)
* [Streaming ZeroMQ](https://github.com/apache/bahir/blob/master/streaming-zeromq/README.md)

Furthermore, to generate scaladocs for each module:

`$ mvn package`

Scaladocs is generated in, `MODULE_NAME/target/site/scaladocs/index.html`.  __ Where `MODULE_NAME` is one of, `sql-streaming-mqtt`, `streaming-akka`, `streaming-mqtt`, `streaming-zeromq`, `streaming-twitter`. __

## A note about Apache Spark integration

Currently, each module in Bahir is available through spark packages. Please follow linking sub section in module specific [README.md](#documentation) for more details.
