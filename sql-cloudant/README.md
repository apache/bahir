Spark Cloudant Connector
================

Cloudant integration with Spark as Spark SQL external datasource, and Spark Streaming as a custom receiver. 


##  Contents:
0. [Linking](#Linking)
1. [Implementation of RelationProvider](#implementation-of-relationProvider)
2. [Implementation of Receiver](#implementation-of-Receiver)
3. [Sample applications](#Sample-application)
    1. [Using SQL In Python](#Using-SQL-In-Python)
    2. [Using SQL In Scala](#Using-SQL-In-Scala)
    3. [Using DataFrame In Python](#Using-DataFrame-In-Python)
    4. [Using DataFrame In Scala](#Using-DataFrame-In-Scala)
    5. [Using Streams In Scala](#Using-Streams-In-Scala)
4. [Configuration Overview](#Configuration-Overview)
5. [Known limitations and areas for improvement](#Known-limitations)


<div id='Linking'/>

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-sql-cloudant" % "2.2.0-SNAPSHOT"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-sql-cloudant_2.11</artifactId>
        <version>2.2.0-SNAPSHOT</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-sql-cloudant_2.11:2.2.0-SNAPSHOT

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is compiled for Scala 2.11 only, and intends to support Spark 2.0 onwards.


<div id='implementation-of-relationProvider'/>

### Implementation of RelationProvider

[DefaultSource.scala](src/main/scala/org/apache/bahir/cloudant/DefaultSource.scala) is a RelationProvider for loading data from Cloudant to Spark, and saving it back from Cloudant to Spark.  It has the following functionalities:

Functionality | Enablement 
--- | ---
Table Option | database or path, search index, view 
Scan Type | PrunedFilteredScan 
Column Pruning | yes
Predicates Push Down | _id or first predicate 
Parallel Loading | yes, except with search index
Insert-able | yes
 

<div id='implementation-of-Receiver'/>

### Implementation of Receiver

Spark Cloudant connector creates a discretized stream in Spark (Spark input DStream) out of Cloudant data sources. [CloudantReceiver.scala](src/main/scala/org/apache/bahir/cloudant/CloudantReceiver.scala) is a custom Receiver that converts `_changes` feed from a Cloudant database to DStream in Spark. This allows all sorts of processing on this streamed data including [using DataFrames and SQL operations on it](examples/src/main/scala/org/apache/spark/examples/sql/cloudant/CloudantStreaming.scala).

**NOTE:** Since CloudantReceiver for Spark Streaming is based on `_changes` API, there are some limitations that application developers should be aware of. Firstly, results returned from `_changes` are partially ordered, and may not be presented in order in which documents were updated. Secondly, in case of shards' unavailability, you may see duplicates, changes that have been seen already. Thus, it is up to applications using Spark Streaming with CloudantReceiver to keep track of _changes they have processed and detect duplicates. 


<div id='Sample-application'/>

## Sample applications

<div id='Using-SQL-In-Python'/>

### Using SQL In Python 
	
[CloudantApp.py](examples/python/CloudantApp.py)

```python
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using temp tables")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .getOrCreate()


#### Loading temp table from Cloudant db
spark.sql(" CREATE TEMPORARY TABLE airportTable USING org.apache.bahir.cloudant OPTIONS ( database 'n_airportcodemapping')")
airportData = spark.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
airportData.printSchema()
print 'Total # of rows in airportData: ' + str(airportData.count())
for code in airportData.collect():
    print code._id
```	

<div id='Using-SQL-In-Scala'/>

### Using SQL In Scala 


[CloudantApp.scala](examples/src/main/scala/org/apache/spark/examples/sql/cloudant/CloudantApp.scala)

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
    |USING org.apache.bahir.cloudant.spark
    |OPTIONS ( database 'n_airportcodemapping')
    """.stripMargin)
// create a dataframe
val airportData = spark.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
airportData.printSchema()
println(s"Total # of rows in airportData: " + airportData.count())
// convert dataframe to array of Rows, and process each row
airportData.map(t => "code: " + t(0) + ",name:" + t(1)).collect().foreach(println)
	
```	


<div id='Using-DataFrame-In-Python'/>	

### Using DataFrame In Python 

[CloudantDF.py](examples/python/CloudantDF.py). 

```python	    
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using dataframes")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .config("jsonstore.rdd.partitions", 8)\
    .getOrCreate()

#### Loading dataframe from Cloudant db
df = spark.read.load("n_airportcodemapping", "org.apache.bahir.cloudant")
df.cache() 
df.printSchema()
df.filter(df.airportName >= 'Moscow').select("_id",'airportName').show()
df.filter(df._id >= 'CAA').select("_id",'airportName').show()	    
```
	
In case of doing multiple operations on a dataframe (select, filter etc.),
you should persist a dataframe. Otherwise, every operation on a dataframe will load the same data from Cloudant again.
Persisting will also speed up computation. This statement will persist an RDD in memory: `df.cache()`.  Alternatively for large dbs to persist in memory & disk, use: 

```python
from pyspark import StorageLevel
df.persist(storageLevel = StorageLevel(True, True, False, True, 1))
```	

[Sample code on using DataFrame option to define cloudant configuration](examples/python/CloudantDFOption.py)

<div id='Using-DataFrame-In-Scala'/>	

### Using DataFrame In Scala 

[CloudantDF.scala](examples/src/main/scala/org/apache/spark/examples/sql/cloudant/CloudantDF.scala)

```	scala
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
    
 [Sample code on using DataFrame option to define cloudant configuration](examples/src/main/scala/org/apache/spark/examples/sql/cloudant/CloudantDFOption.scala)
 
 
<div id='Using-Streams-In-Scala'/>
 
### Using Streams In Scala 
[CloudantStreaming.scala](examples/src/main/scala/org/apache/spark/examples/sql/cloudant/CloudantStreaming.scala)

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

By default, Spark Streaming will load all documents from a database. If you want to limit the loading to specific documents, use `selector` option of `CloudantReceiver` and specify your conditions ([CloudantStreamingSelector.scala](examples/src/main/scala/org/apache/spark/examples/sql/cloudant/CloudantStreamingSelector.scala)):

```scala
val changes = ssc.receiverStream(new CloudantReceiver(Map(
  "cloudant.host" -> "ACCOUNT.cloudant.com",
  "cloudant.username" -> "USERNAME",
  "cloudant.password" -> "PASSWORD",
  "database" -> "sales",
  "selector" -> "{\"month\":\"May\", \"rep\":\"John\"}")))
```


<div id='Configuration-Overview'/>

## Configuration Overview	

The configuration is obtained in the following sequence:

1. default in the Config, which is set in the application.conf
2. key in the SparkConf, which is set in SparkConf
3. key in the parameters, which is set in a dataframe or temporaty table options, or StreamReceiver
4. "spark."+key in the SparkConf (as they are treated as the one passed in through spark-submit using --conf option)

Here each subsequent configuration overrides the previous one. Thus, configuration set using DataFrame option overrides what has beens set in SparkConf. And configuration passed in spark-submit using --conf takes precedence over any setting in the code. When passing configuration in spark-submit, make sure adding "spark." as prefix to the keys.


### Configuration in application.conf

Default values are defined in [here](src/main/resources/application.conf)

### Configuration on SparkConf

Name | Default | Meaning
--- |:---:| ---
cloudant.protocol|https|protocol to use to transfer data: http or https
cloudant.host||cloudant host url
cloudant.username||cloudant userid
cloudant.password||cloudant password
jsonstore.rdd.partitions|10|the number of partitions intent used to drive JsonStoreRDD loading query result in parallel. The actual number is calculated based on total rows returned and satisfying maxInPartition and minInPartition
jsonstore.rdd.maxInPartition|-1|the max rows in a partition. -1 means unlimited
jsonstore.rdd.minInPartition|10|the min rows in a partition.
jsonstore.rdd.requestTimeout|900000| the request timeout in milliseconds
bulkSize|200| the bulk save size
schemaSampleSize| "-1" | the sample size for RDD schema discovery. 1 means we are using only first document for schema discovery; -1 means all documents; 0 will be treated as 1; any number N means min(N, total) docs 
createDBOnSave|"false"| whether to create a new database during save operation. If false, a database should already exist. If true, a new database will be created. If true, and a database with a provided name already exists, an error will be raised. 


###  Configuration on Spark SQL Temporary Table or DataFrame

Besides overriding any SparkConf configuration, you can also set the following configurations at temporary table or dataframe level.

Name | Default | Meaning
--- |:---:| ---
database||cloudant database name
view||cloudant view w/o the database name. only used for load.  
index||cloudant search index w/o the database name. only used for load data with less than or equal to 200 results.
path||cloudant: as database name if database is not present


#### View Specific

For fast loading, views are loaded without include_docs. Thus, a derived schema will always be: `{id, key, value}`, where `value `can be a compound field. An example of loading data from a view:

```python
spark.sql(" CREATE TEMPORARY TABLE flightTable1 USING org.apache.bahir.cloudant OPTIONS ( database 'n_flight', view '_design/view/_view/AA0')")

```

###  Configuration on Cloudant Receiver for Spark Streaming

Besides overriding any SparkConf configuration, you can also set the following configurations at stream Receiver level

Name | Default | Meaning
--- |:---:| ---
database||cloudant database name
selector| all documents| a selector written in Cloudant Query syntax, specifying conditions for selecting documents. Only documents satisfying the selector's conditions will be retrieved from Cloudant and loaded into Spark.


<div id='Known-limitations'/>

## Known limitations and areas for improvement

* Loading data from Cloudant search index will work only for up to 200 results.
		
* Need to improve how number of partitions is determined for parallel loading
