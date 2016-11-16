A custom data source to read and write data from and to remote HDFS clusters using the [WebHDFS](https://hadoop.apache.org/docs/r2.7.3/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) protocol. 

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-datasource-webhdfs" % "2.1.0-SNAPSHOT"

Using Maven:

```xml
<dependency>
    <groupId>org.apache.bahir</groupId>
    <artifactId>spark-datasource-webhdfs_2.11</artifactId>
    <version>2.1.0-SNAPSHOT</version>
</dependency>
```

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-datasource-webhdfs_2.11:2.1.0-SNAPSHOT

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is compiled for Scala 2.11 only, and intends to support Spark 2.0 onwards.

## Examples

A data frame can be created using this custom data source as shown below -

```scala
val filePath = s"webhdfs://<server name or ip>/gateway/default/webhdfs/v1/<file or folder name>"

val df = spark.read
    .format("webhdfs")
    .option("certValidation", "Y")
    .option("userCred", "user1:pass1")
    .option("header", "true")
    .option("partitions", "8")
    .load(filePath)
```

## Configuration options.

 * `certValidation` Set this to `'Y'` or `'N'`. In case of `'N'` this component will ignore validation of teh SSL certification. Otherwise it will download the certificate and validate.
 * `userCred` Set this to `'userid:password'` as needed by the remote HDFS for accessing a file from there.
 * `partitions` This number tells the Data Source how many parallel connections to be opened to read data from HDFS in the remote cluster for each file. If this option is not specified default value is used which is 4. Recommended value for this option is same as the next nearest integer of (file size/block size) in HDFS or multiples of that. For example if file size in HDFS is 0.95 GB and block size of the file is 128 MB use 8 or multiple of 8 as number of partitions. However, number of partitions should not be more than (or may be little more than) maximum number of parallel tasks possible to spawn in your Spark cluster. 
 * `format` Format of the file. Right now only 'csv' is supported. If this option is not specified by default 'csv' is assumed.
 * `output`  Specify either `'LIST'` or `'Data'`. By default, `'Data'` is assumed which returns the actual data in the file. If a folder name is specified then data from all files in that folder would be fetched at once. If `'LIST'` is specified then the files within the folder is listed.
