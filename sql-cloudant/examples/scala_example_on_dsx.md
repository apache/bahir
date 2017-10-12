
# Scala example using Spark SQL over Cloudant as a source

This sample notebook is written in Scala and expects the Scala 2.11 runtime with Spark 2.1 or later.

The data source for this example can be found at: http://examples.cloudant.com/crimes/

Replicate the database into your own Cloudant account before you execute this script.

## 1. Work with the Spark Context

A Spark Context handle `sc` is available with every notebook create in the Spark Service. Use it to understand the Spark version used, the environment settings, and create a Spark SQL Context object off of it.


```scala
sc.version
```
    2.1.0



## 2. Work with a Cloudant database

A Dataframe object can be created directly from a Cloudant database. To configure the database as source, pass these options:

1 - package name that provides the classes (like `CloudantDataSource`) implemented in the connector to extend `BaseRelation`. For the Cloudant Spark connector this will be `org.apache.bahir.cloudant`

2 - `cloudant.host` parameter to pass the Cloudant account name

3 - `cloudant.user` parameter to pass the Cloudant user name

4 - `cloudant.password` parameter to pass the Cloudant account password


```scala
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val df = sqlContext.read.format("org.apache.bahir.cloudant").
option("cloudant.host","USERNAME.cloudant.com").
option("cloudant.username","USERNAME").
option("cloudant.password","PASSWORD").
load("crimes")
```

    [Stage 0:====================================================>     (9 + 1) / 10]

## 3. Work with a Dataframe

At this point all transformations and functions should behave as specified with Spark SQL. (http://spark.apache.org/sql/)

There are, however, a number of things the Cloudant Spark connector does not support yet, or things that are simply not working. For that reason we call this connector a **BETA** release and are only gradually improving it towards GA. 

Please direct your any change requests at [support@cloudant.com](mailto:support@cloudant.com)


```scala
df.printSchema()
```

    root
     |-- _id: string (nullable = true)
     |-- _rev: string (nullable = true)
     |-- geometry: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- properties: struct (nullable = true)
     |    |-- compnos: string (nullable = true)
     |    |-- domestic: boolean (nullable = true)
     |    |-- fromdate: long (nullable = true)
     |    |-- main_crimecode: string (nullable = true)
     |    |-- name: string (nullable = true)
     |    |-- naturecode: string (nullable = true)
     |    |-- reptdistrict: string (nullable = true)
     |    |-- shooting: boolean (nullable = true)
     |    |-- source: string (nullable = true)
     |-- type: string (nullable = true)
    



```scala
df.count()
```

    [Stage 1:==============================================>           (8 + 2) / 10]




    269




```scala
df.select("properties.naturecode").show()
```

    +----------+
    |naturecode|
    +----------+
    |    DISTRB|
    |       EDP|
    |    ARREST|
    |        AB|
    |      CD14|
    |    UNKEMS|
    |      REQP|
    |       EDP|
    |       MVA|
    |     IVPER|
    |      NIDV|
    |        AB|
    |    IVPREM|
    |     IVPER|
    |     IVPER|
    |       MVA|
    |      CD11|
    |    LARCEN|
    |       MVA|
    |    ARREST|
    +----------+
    only showing top 20 rows
    



```scala
df.filter(df.col("properties.naturecode").startsWith("DISTRB")).show()
```

    [Stage 5:============================================>              (3 + 1) / 4]+--------------------+--------------------+--------------------+--------------------+-------+
    |                 _id|                _rev|            geometry|          properties|   type|
    +--------------------+--------------------+--------------------+--------------------+-------+
    |79f14b64c57461584...|1-d9518df5c255e4b...|[WrappedArray(-71...|[142035012,true,1...|Feature|
    |79f14b64c57461584...|1-798ef404b141dfb...|[WrappedArray(-71...|[142035053,false,...|Feature|
    |79f14b64c57461584...|1-08cd46894f9c579...|[WrappedArray(-71...|[142035113,false,...|Feature|
    |79f14b64c57461584...|1-be4512491803441...|[WrappedArray(-71...|[142035116,false,...|Feature|
    |79f14b64c57461584...|1-2e3e1fe35278b5d...|[WrappedArray(-71...|[142035162,false,...|Feature|
    |79f14b64c57461584...|1-e03133da93c2644...|[WrappedArray(-71...|[142035211,false,...|Feature|
    |79f14b64c57461584...|1-4c21d07bfb9f45a...|[WrappedArray(-71...|[142035316,false,...|Feature|
    +--------------------+--------------------+--------------------+--------------------+-------+
    

