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

spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using dataframes with options")\
    .getOrCreate()

cloudant_host = "ACCOUNT.cloudant.com"
cloudant_username = "USERNAME"
cloudant_password = "PASSWORD"

# ***1. Loading dataframe from Cloudant db
df = spark.read.format("org.apache.bahir.cloudant") \
    .option("cloudant.host", cloudant_host) \
    .option("cloudant.username", cloudant_username) \
    .option("cloudant.password", cloudant_password) \
    .load("n_airportcodemapping")
df.cache() # persisting in memory
df.printSchema()
df.filter(df._id >= 'CAA').select("_id",'airportName').show()


# ***2.Saving dataframe to Cloudant db
df.filter(df._id >= 'CAA').select("_id",'airportName') \
    .write.format("org.apache.bahir.cloudant") \
    .option("cloudant.host", cloudant_host) \
    .option("cloudant.username", cloudant_username) \
    .option("cloudant.password",cloudant_password) \
    .option("bulkSize","100") \
    .option("createDBOnSave", "true") \
    .save("airportcodemapping_df")
df = spark.read.format("org.apache.bahir.cloudant") \
    .option("cloudant.host", cloudant_host) \
    .option("cloudant.username", cloudant_username) \
    .option("cloudant.password", cloudant_password) \
    .load("n_flight")
df.printSchema()
total = df.filter(df.flightSegmentId >'AA9') \
    .select("flightSegmentId", "scheduledDepartureTime") \
    .orderBy(df.flightSegmentId).count()
print "Total", total, "flights from table"


# ***3. Loading dataframe from Cloudant search index
df = spark.read.format("org.apache.bahir.cloudant") \
    .option("cloudant.host",cloudant_host) \
    .option("cloudant.username",cloudant_username) \
    .option("cloudant.password",cloudant_password) \
    .option("index","_design/view/_search/n_flights").load("n_flight")
df.printSchema()

total = df.filter(df.flightSegmentId >'AA9') \
    .select("flightSegmentId", "scheduledDepartureTime") \
    .orderBy(df.flightSegmentId).count()
print "Total", total, "flights from index"
