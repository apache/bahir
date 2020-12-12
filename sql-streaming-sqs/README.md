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
# Spark SQL Streaming Amazon SQS Data Source

A library for reading data from Amzon S3 with optimised listing using Amazon SQS using Spark SQL Streaming ( or Structured streaming.).

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-sql-streaming-sqs" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-sql-streaming-sqs_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-sql-streaming-sqs_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is compiled for Scala 2.12 only, and intends to support Spark 2.4.0 onwards.

## Configuration options
The configuration is obtained from parameters.

Name |Default | Meaning
--- |:---:| ---
sqsUrl|required, no default value|sqs queue url, like 'https://sqs.us-east-1.amazonaws.com/330183209093/TestQueue'
region|required, no default value|AWS region where queue is created
fileFormat|required, no default value|file format for the s3 files stored on Amazon S3
schema|required, no default value|schema of the data being read
sqsFetchIntervalSeconds|10|time interval (in seconds) after which to fetch messages from Amazon SQS queue
sqsLongPollingWaitTimeSeconds|20|wait time (in seconds) for long polling on Amazon SQS queue
sqsMaxConnections|1|number of parallel threads to connect to Amazon SQS queue
sqsMaxRetries|10|Maximum number of consecutive retries in case of a connection failure to SQS before giving up
ignoreFileDeletion|false|whether to ignore any File deleted message in SQS queue
fileNameOnly|false|Whether to check new files based on only the filename instead of on the full path
shouldSortFiles|true|whether to sort files based on timestamp while listing them from SQS
useInstanceProfileCredentials|false|Whether to use EC2 instance profile credentials for connecting to Amazon SQS
maxFilesPerTrigger|no default value|maximum number of files to process in a microbatch
maxFileAge|7d|Maximum age of a file that can be found in this directory
messageWrapper|None| - 'None' if SQS contains plain S3 message. <br/> - 'SNS' if SQS contains S3 notification message which came from SNS. <br/> Please see 'Use multiple consumers' section for more details 

## Use multiple consumers

SQS cannot be read by multiple consumers. <br/> 
If S3 path should be listen by multiple applications the following approach is recommended: S3 -> SNS -> SQS:
1. Create multiple SQS queues. Each application listen for one SQS queue.
2. Create 1 SNS topic
3. Once S3 notification event is pushed to SNS topic it will be delivered to each SQS queue 

Thus, one S3 path can be processed by multiple applications. <br/>
 

## Example

An example to create a SQL stream which uses Amazon SQS to list files on S3,

        val inputDf = sparkSession
                          .readStream
                          .format("s3-sqs")
                          .schema(schema)
                          .option("sqsUrl", queueUrl)
                          .option("fileFormat", "json")
                          .option("sqsFetchIntervalSeconds", "2")
                          .option("sqsLongPollingWaitTimeSeconds", "5")
                          .option("useInstanceProfileCredentials", "true")
                          .load()
