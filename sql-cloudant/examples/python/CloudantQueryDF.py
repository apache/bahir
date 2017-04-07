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
    .appName("Cloudant Spark SQL Example in Python using query")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .config("jsonstore.rdd.partitions", 8)\
    .config("cloudant.useQuery", "true")\
    .config("schemaSampleSize",1)\
    .getOrCreate()


# ***0. Loading dataframe from Cloudant db with one String field condition
df = spark.read.load("n_airportcodemapping", "org.apache.bahir.cloudant")
df.printSchema()
df.filter(df.airportName == 'Moscow').select("_id",'airportName').show()


# ***1. Loading dataframe from Cloudant db with one String field condition
df = spark.read.load("n_airportcodemapping", "org.apache.bahir.cloudant")
df.printSchema()
df.filter(df.airportName > 'Moscow').select("_id",'airportName').show()

# ***2. Loading dataframe from Cloudant db with two String field condition
df = spark.read.load("n_airportcodemapping", "org.apache.bahir.cloudant")
df.printSchema()
df.filter(df.airportName > 'Moscow').filter(df.airportName < 'Sydney').select("_id",'airportName').show()

# ***3. Loading dataframe from Cloudant db with two int field condition
df = spark.read.load("n_flight", "org.apache.bahir.cloudant")
df.printSchema()
df.filter(df.economyClassBaseCost >= 200).filter(df.numFirstClassSeats <=10).select('flightSegmentId','scheduledDepartureTime', 'scheduledArrivalTime').show()

# ***4. Loading dataframe from Cloudant db with two timestamp field condition
df = spark.read.load("n_flight", "org.apache.bahir.cloudant")
df.printSchema()
df.filter(df.scheduledDepartureTime >= "2014-12-15T05:00:00.000Z").filter(df.scheduledArrivalTime <="2014-12-15T11:04:00.000Z").select('flightSegmentId','scheduledDepartureTime', 'scheduledArrivalTime').show()


