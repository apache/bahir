A library for reading data from Amzon S3 with optimised listing using Amazon SQS using Spark SQL Streaming ( or Structured streaming.). 

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-sql-streaming-sqs" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-sql-streaming-akka_{{site.SCALA_BINARY_VERSION}}</artifactId>
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
                          .load()
