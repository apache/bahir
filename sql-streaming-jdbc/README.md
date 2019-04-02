A library for writing data to jdbc using Spark SQL Streaming (or Structured streaming).

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-sql-streaming-jdbc" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-sql-streaming-jdbc_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-sql-streaming-jdbc_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is compiled for Scala 2.11 only, and intends to support Spark 2.0 onwards.

## Configuration options
The configuration is obtained from parameters.

Name |Default | Meaning
--- |:---:| ---
url|required, no default value|jdbc url, like 'jdbc:mysql://127.0.0.1:3306/test?characterEncoding=UTF8'
dbtable|required, no default value|table name
driver|Attempts to locate a driver that understands the given URL by DriverManager, if driver parameter not specificed|full driver class name, like 'com.mysql.jdbc.Driver'
user|None|username for database
password|None|password for database
batchsize|1000|records is batched writted to jdbc, to decrease jdbc pressure
maxRetryNumber|4|max retry number before a task write to jdbc fails
checkValidTimeoutSeconds|10|We cache a connection to avoid creating a new jdbc connection for each batch, timeout for checking connection valid

## Examples

### Scala API
An example, for scala API to count words from incoming message stream.

    // Create DataFrame from some stream source
    val query = df.writeStream
        .format("streaming-jdbc")
        .option("checkpointLocation", "/path/to/localdir")
        .outputMode("Append")
        .option("url", "my jdbc url")
        .option("dbtable", "myTableName")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "my database username")
        .option("password", "my database password")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start()

    query.awaitTermination()

Please see `JdbcSinkDemo.scala` for full example.

### Java API
An example, for Java API to count words from incoming message stream.

    StreamingQuery query = result
        .writeStream()
        .outputMode("append")
        .format("streaming-jdbc")
        .outputMode(OutputMode.Append())
        .option(JDBCOptions.JDBC_URL(), jdbcUrl)
        .option(JDBCOptions.JDBC_TABLE_NAME(), tableName)
        .option(JDBCOptions.JDBC_DRIVER_CLASS(), "com.mysql.jdbc.Driver")
        .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), "5")
        .option("user", username)
        .option("password", password)
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start();
    query.awaitTermination();

Please see `JavaJdbcSinkDemo.java` for full example.
