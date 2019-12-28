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
# Spark Streaming Google Cloud Pub/Sub Connector

A library for reading data from [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/) using Spark Streaming.

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-streaming-pubsub" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-streaming-pubsub_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-streaming-pubsub_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

## Examples

First you need to create credential by SparkGCPCredentials, it support four type of credentials
* application default
    `SparkGCPCredentials.builder.build()`
* JSON type service account (based on file or its binary content)
    `SparkGCPCredentials.builder.jsonServiceAccount(PATH_TO_JSON_KEY).build()`
    `SparkGCPCredentials.builder.jsonServiceAccount(JSON_KEY_BYTES).build()`
* P12 type service account
    `SparkGCPCredentials.builder.p12ServiceAccount(PATH_TO_P12_KEY, EMAIL_ACCOUNT).build()`
    `SparkGCPCredentials.builder.p12ServiceAccount(P12_KEY_BYTES, EMAIL_ACCOUNT).build()`
* Metadata service account (running on dataproc)
    `SparkGCPCredentials.builder.metadataServiceAccount().build()`

### Scala API

    val lines = PubsubUtils.createStream(ssc, projectId, subscriptionName, credential, ..)

### Java API

    JavaDStream<SparkPubsubMessage> lines = PubsubUtils.createStream(jssc, projectId, subscriptionName, credential...)

See end-to-end examples at [Google Cloud Pubsub Examples](streaming-pubsub/examples)

### Unit Test

To run the PubSub test cases, you need to generate **Google API service account key files** and set the corresponding environment variable to enable the test.

#### To generate a service account key file with PubSub permission

1. Go to [Google API Console](console.cloud.google.com)
2. Choose the `Credentials` Tab> `Create credentials` button> `Service account key`
3. Fill the account name, assign `Role> Pub/Sub> Pub/Sub Editor` and check the option `Furnish a private key` to create one. You need to create one for JSON key file, another for P12.
4. The account email is the `Service account ID`

#### Setting the environment variables and run test

```
mvn clean package -DskipTests -pl streaming-pubsub

export ENABLE_PUBSUB_TESTS=1
export GCP_TEST_ACCOUNT="THE_P12_SERVICE_ACCOUNT_ID_MENTIONED_ABOVE"
export GCP_TEST_PROJECT_ID="YOUR_GCP_PROJECT_ID"
export GCP_TEST_JSON_KEY_PATH=/path/to/pubsub/credential/files/Apache-Bahir-PubSub-1234abcd.json
export GCP_TEST_P12_KEY_PATH=/path/to/pubsub/credential/files/Apache-Bahir-PubSub-5678efgh.p12

mvn test -pl streaming-pubsub
```
