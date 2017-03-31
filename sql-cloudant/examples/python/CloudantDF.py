#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pprint
from pyspark.sql import SparkSession

# define cloudant related configuration
# set protocol to http if needed, default value=https
# config("cloudant.protocol","http")
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
# In case of doing multiple operations on a dataframe (select, filter etc.)
# you should persist the dataframe.
# Othewise, every operation on the dataframe will load the same data from Cloudant again.
# Persisting will also speed up computation.
df.cache() # persisting in memory
# alternatively for large dbs to persist in memory & disk:
# from pyspark import StorageLevel
# df.persist(storageLevel = StorageLevel(True, True, False, True, 1)) 
df.printSchema()
df.filter(df.airportName >= 'Moscow').select("_id",'airportName').show()
df.filter(df._id >= 'CAA').select("_id",'airportName').show()


# ***2. Saving a datafram to Cloudant db
df = spark.read.load(format="org.apache.bahir.cloudant", database="n_flight")
df.printSchema()
df2 = df.filter(df.flightSegmentId=='AA106')\
    .select("flightSegmentId", "economyClassBaseCost")
df2.write.save("n_flight2",  "org.apache.bahir.cloudant",
        bulkSize = "100", createDBOnSave="true") 
total = df.filter(df.flightSegmentId >'AA9').select("flightSegmentId", 
        "scheduledDepartureTime").orderBy(df.flightSegmentId).count()
print "Total", total, "flights from table"


# ***3. Loading dataframe from a Cloudant search index
df = spark.read.load(format="org.apache.bahir.cloudant", database="n_flight", 
        index="_design/view/_search/n_flights")
df.printSchema()
total = df.filter(df.flightSegmentId >'AA9').select("flightSegmentId", 
        "scheduledDepartureTime").orderBy(df.flightSegmentId).count()
print "Total", total, "flights from index"


# ***4. Loading dataframe from a Cloudant view
df = spark.read.load(format="org.apache.bahir.cloudant", path="n_flight", 
        view="_design/view/_view/AA0", schemaSampleSize="20")
# schema for view will always be: _id, key, value
# where value can be a complex field
df.printSchema()
df.show()
