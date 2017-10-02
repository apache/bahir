A library for reading data from Cloudant or CouchDB databases using Spark SQL and Spark Streaming. 

[IBM® Cloudant®](https://cloudant.com) is a document-oriented DataBase as a Service (DBaaS). It stores data as documents 
in JSON format. It's built with scalability, high availability, and durability in mind. It comes with a 
wide variety of indexing options including map-reduce, Cloudant Query, full-text indexing, and 
geospatial indexing. The replication capabilities make it easy to keep data in sync between database 
clusters, desktop PCs, and mobile devices.

[Apache CouchDB™](http://couchdb.apache.org) is open source database software that focuses on ease of use and having an architecture that "completely embraces the Web". It has a document-oriented NoSQL database architecture and is implemented in the concurrency-oriented language Erlang; it uses JSON to store data, JavaScript as its query language using MapReduce, and HTTP for an API.

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-sql-cloudant" % "{{site.SPARK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-sql-cloudant_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.SPARK_VERSION}}</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.

    $ bin/spark-shell --packages org.apache.bahir:spark-sql-cloudant_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

Submit a job in Python:
    
    spark-submit  --master local[4] --packages org.apache.bahir:spark-sql-cloudant__{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}  <path to python script>
    
Submit a job in Scala:

	spark-submit --class "<your class>" --master local[4] --packages org.apache.bahir:spark-sql-cloudant__{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}} <path to spark-sql-cloudant jar>

This library is compiled for Scala 2.11 only, and intends to support Spark 2.0 onwards.

## Configuration options	
The configuration is obtained in the following sequence:

1. default in the Config, which is set in the application.conf
2. key in the SparkConf, which is set in SparkConf
3. key in the parameters, which is set in a dataframe or temporaty table options
4. "spark."+key in the SparkConf (as they are treated as the one passed in through spark-submit using --conf option)

Here each subsequent configuration overrides the previous one. Thus, configuration set using DataFrame option overrides what has beens set in SparkConf. And configuration passed in spark-submit using --conf takes precedence over any setting in the code.


### Configuration in application.conf
Default values are defined in [here](src/main/resources/application.conf).

### Configuration on SparkConf

Name | Default | Meaning
--- |:---:| ---
cloudant.batchInterval|8|number of seconds to set for streaming all documents from `_changes` endpoint into Spark dataframe.  See [Setting the right batch interval](https://spark.apache.org/docs/latest/streaming-programming-guide.html#setting-the-right-batch-interval) for tuning this value.
cloudant.endpoint|`_all_docs`|endpoint for RelationProvider when loading data from Cloudant to DataFrames or SQL temporary tables. Select between the Cloudant `_all_docs` or `_changes` API endpoint.  See **Note** below for differences between endpoints.
cloudant.protocol|https|protocol to use to transfer data: http or https
cloudant.host| |cloudant host url
cloudant.username| |cloudant userid
cloudant.password| |cloudant password
cloudant.useQuery|false|by default, `_all_docs` endpoint is used if configuration 'view' and 'index' (see below) are not set. When useQuery is enabled, `_find` endpoint will be used in place of `_all_docs` when query condition is not on primary key field (_id), so that query predicates may be driven into datastore. 
cloudant.queryLimit|25|the maximum number of results returned when querying the `_find` endpoint.
storageLevel|MEMORY_ONLY|the storage level for persisting Spark RDDs during load when `cloudant.endpoint` is set to `_changes`.  See [RDD Persistence section](https://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence) in Spark's Progamming Guide for all available storage level options.
cloudant.timeout|60000|stop the response after waiting the defined number of milliseconds for data.  Only supported with `changes` endpoint.
jsonstore.rdd.partitions|10|the number of partitions intent used to drive JsonStoreRDD loading query result in parallel. The actual number is calculated based on total rows returned and satisfying maxInPartition and minInPartition. Only supported with `_all_docs` endpoint.
jsonstore.rdd.maxInPartition|-1|the max rows in a partition. -1 means unlimited
jsonstore.rdd.minInPartition|10|the min rows in a partition.
jsonstore.rdd.requestTimeout|900000|the request timeout in milliseconds
bulkSize|200|the bulk save size
schemaSampleSize|-1|the sample size for RDD schema discovery. 1 means we are using only the first document for schema discovery; -1 means all documents; 0 will be treated as 1; any number N means min(N, total) docs. Only supported with `_all_docs` endpoint.
createDBOnSave|false|whether to create a new database during save operation. If false, a database should already exist. If true, a new database will be created. If true, and a database with a provided name already exists, an error will be raised. 

The `cloudant.endpoint` option sets ` _changes` or `_all_docs` API endpoint to be called while loading Cloudant data into Spark DataFrames or SQL Tables.

**Note:** When using `_changes` API, please consider: 
1. Results are partially ordered and may not be be presented in order in 
which documents were updated.
2. In case of shards' unavailability, you may see duplicate results (changes that have been seen already)
3. Can use `selector` option to filter Cloudant docs during load
4. Supports a real snapshot of the database and represents it in a single point of time.
5. Only supports a single partition.


When using `_all_docs` API:
1. Supports parallel reads (using offset and range) and partitioning.
2. Using partitions may not represent the true snapshot of a database.  Some docs
   may be added or deleted in the database between loading data into different 
   Spark partitions.

If loading Cloudant docs from a database greater than 100 MB, set `cloudant.endpoint` to `_changes` and `spark.streaming.unpersist` to `false`.
This will enable RDD persistence during load against `_changes` endpoint and allow the persisted RDDs to be accessible after streaming completes.  
 
See [CloudantChangesDFSuite](src/test/scala/org/apache/bahir/cloudant/CloudantChangesDFSuite.scala) 
for examples of loading data into a Spark DataFrame with `_changes` API.

### Configuration on Spark SQL Temporary Table or DataFrame

Besides all the configurations passed to a temporary table or dataframe through SparkConf, it is also possible to set the following configurations in temporary table or dataframe using OPTIONS: 

Name | Default | Meaning
--- |:---:| ---
bulkSize|200| the bulk save size
createDBOnSave|false| whether to create a new database during save operation. If false, a database should already exist. If true, a new database will be created. If true, and a database with a provided name already exists, an error will be raised. 
database| | Cloudant database name
index| | Cloudant Search index without the database name. Search index queries are limited to returning 200 results so can only be used to load data with <= 200 results.
path| | Cloudant: as database name if database is not present
schemaSampleSize|-1| the sample size used to discover the schema for this temp table. -1 scans all documents
selector|all documents| a selector written in Cloudant Query syntax, specifying conditions for selecting documents when the `cloudant.endpoint` option is set to `_changes`. Only documents satisfying the selector's conditions will be retrieved from Cloudant and loaded into Spark.
view| | Cloudant view w/o the database name. only used for load.

For fast loading, views are loaded without include_docs. Thus, a derived schema will always be: `{id, key, value}`, where `value `can be a compount field. An example of loading data from a view: 

```python
spark.sql(" CREATE TEMPORARY TABLE flightTable1 USING org.apache.bahir.cloudant OPTIONS ( database 'n_flight', view '_design/view/_view/AA0')")

```

### Configuration on Cloudant Receiver for Spark Streaming

Name | Default | Meaning
--- |:---:| ---
cloudant.host||cloudant host url
cloudant.username||cloudant userid
cloudant.password||cloudant password
database||cloudant database name
selector| all documents| a selector written in Cloudant Query syntax, specifying conditions for selecting documents. Only documents satisfying the selector's conditions will be retrieved from Cloudant and loaded into Spark.

### Configuration in spark-submit using --conf option

The above stated configuration keys can also be set using `spark-submit --conf` option. When passing configuration in spark-submit, make sure adding "spark." as prefix to the keys.


## Examples

### Python API

#### Using SQL In Python 
	
```python
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using temp tables")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .getOrCreate()


# Loading temp table from Cloudant db
spark.sql(" CREATE TEMPORARY TABLE airportTable USING org.apache.bahir.cloudant OPTIONS ( database 'n_airportcodemapping')")
airportData = spark.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
airportData.printSchema()
print 'Total # of rows in airportData: ' + str(airportData.count())
for code in airportData.collect():
    print code._id
```

See [CloudantApp.py](examples/python/CloudantApp.py) for examples.

Submit job example:
```
spark-submit  --packages org.apache.bahir:spark-sql-cloudant_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}} --conf spark.cloudant.host=ACCOUNT.cloudant.com --conf spark.cloudant.username=USERNAME --conf spark.cloudant.password=PASSWORD sql-cloudant/examples/python/CloudantApp.py
```

#### Using DataFrame In Python 

```python
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using dataframes")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .config("jsonstore.rdd.partitions", 8)\
    .getOrCreate()

# ***1. Loading dataframe from Cloudant db
df = spark.read.load("n_airportcodemapping", "org.apache.bahir.cloudant")
df.cache() 
df.printSchema()
df.filter(df.airportName >= 'Moscow').select("_id",'airportName').show()
df.filter(df._id >= 'CAA').select("_id",'airportName').show()	    
```

See [CloudantDF.py](examples/python/CloudantDF.py) for examples.
	
In case of doing multiple operations on a dataframe (select, filter etc.),
you should persist a dataframe. Otherwise, every operation on a dataframe will load the same data from Cloudant again.
Persisting will also speed up computation. This statement will persist an RDD in memory: `df.cache()`.  Alternatively for large dbs to persist in memory & disk, use: 

```python
from pyspark import StorageLevel
df.persist(storageLevel = StorageLevel(True, True, False, True, 1))
```

[Sample code](examples/python/CloudantDFOption.py) on using DataFrame option to define cloudant configuration

### Scala API

#### Using SQL In Scala 

```scala
val spark = SparkSession
      .builder()
      .appName("Cloudant Spark SQL Example")
      .config("cloudant.host","ACCOUNT.cloudant.com")
      .config("cloudant.username", "USERNAME")
      .config("cloudant.password","PASSWORD")
      .getOrCreate()

// For implicit conversions of Dataframe to RDDs
import spark.implicits._
    
// create a temp table from Cloudant db and query it using sql syntax
spark.sql(
    s"""
    |CREATE TEMPORARY TABLE airportTable
    |USING org.apache.bahir.cloudant
    |OPTIONS ( database 'n_airportcodemapping')
    """.stripMargin)
// create a dataframe
val airportData = spark.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
airportData.printSchema()
println(s"Total # of rows in airportData: " + airportData.count())
// convert dataframe to array of Rows, and process each row
airportData.map(t => "code: " + t(0) + ",name:" + t(1)).collect().foreach(println)
```
See [CloudantApp.scala](examples/scala/src/main/scala/mytest/spark/CloudantApp.scala) for examples.

Submit job example:
```
spark-submit --class org.apache.spark.examples.sql.cloudant.CloudantApp --packages org.apache.bahir:spark-sql-cloudant_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}} --conf spark.cloudant.host=ACCOUNT.cloudant.com --conf spark.cloudant.username=USERNAME --conf spark.cloudant.password=PASSWORD  /path/to/spark-sql-cloudant_{{site.SCALA_BINARY_VERSION}}-{{site.SPARK_VERSION}}-tests.jar
```

### Using DataFrame In Scala 

```scala
val spark = SparkSession
      .builder()
      .appName("Cloudant Spark SQL Example with Dataframe")
      .config("cloudant.host","ACCOUNT.cloudant.com")
      .config("cloudant.username", "USERNAME")
      .config("cloudant.password","PASSWORD")
      .config("createDBOnSave","true") // to create a db on save
      .config("jsonstore.rdd.partitions", "20") // using 20 partitions
      .getOrCreate()
          
// 1. Loading data from Cloudant db
val df = spark.read.format("org.apache.bahir.cloudant").load("n_flight")
// Caching df in memory to speed computations
// and not to retrieve data from cloudant again
df.cache() 
df.printSchema()

// 2. Saving dataframe to Cloudant db
val df2 = df.filter(df("flightSegmentId") === "AA106")
    .select("flightSegmentId","economyClassBaseCost")
df2.show()
df2.write.format("org.apache.bahir.cloudant").save("n_flight2")
```

See [CloudantDF.scala](examples/scala/src/main/scala/mytest/spark/CloudantDF.scala) for examples.
    
[Sample code](examples/scala/src/main/scala/mytest/spark/CloudantDFOption.scala) on using DataFrame option to define Cloudant configuration.
 
 
### Using Streams In Scala 

```scala
val ssc = new StreamingContext(sparkConf, Seconds(10))
val changes = ssc.receiverStream(new CloudantReceiver(Map(
  "cloudant.host" -> "ACCOUNT.cloudant.com",
  "cloudant.username" -> "USERNAME",
  "cloudant.password" -> "PASSWORD",
  "database" -> "n_airportcodemapping")))

changes.foreachRDD((rdd: RDD[String], time: Time) => {
  // Get the singleton instance of SparkSession
  val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

  println(s"========= $time =========")
  // Convert RDD[String] to DataFrame
  val changesDataFrame = spark.read.json(rdd)
  if (!changesDataFrame.schema.isEmpty) {
    changesDataFrame.printSchema()
    changesDataFrame.select("*").show()
    ....
  }
})
ssc.start()
// run streaming for 120 secs
Thread.sleep(120000L)
ssc.stop(true)
	
```

See [CloudantStreaming.scala](examples/scala/src/main/scala/mytest/spark/CloudantStreaming.scala) for examples.

By default, Spark Streaming will load all documents from a database. If you want to limit the loading to 
specific documents, use `selector` option of `CloudantReceiver` and specify your conditions 
(See [CloudantStreamingSelector.scala](examples/scala/src/main/scala/mytest/spark/CloudantStreamingSelector.scala)
example for more details):

```scala
val changes = ssc.receiverStream(new CloudantReceiver(Map(
  "cloudant.host" -> "ACCOUNT.cloudant.com",
  "cloudant.username" -> "USERNAME",
  "cloudant.password" -> "PASSWORD",
  "database" -> "sales",
  "selector" -> "{\"month\":\"May\", \"rep\":\"John\"}")))
```
