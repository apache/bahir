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


spark.sql(" CREATE TEMPORARY VIEW airportTable1 USING org.apache.bahir.cloudant OPTIONS ( database 'n_airportcodemapping')")
airportData = spark.sql("SELECT _id, airportName FROM airportTable1 WHERE airportName == 'Moscow' ")
airportData.printSchema()
print 'Total # of rows in airportData: ' + str(airportData.count())
airportData.show()

spark.sql(" CREATE TEMPORARY VIEW airportTable2 USING org.apache.bahir.cloudant OPTIONS ( database 'n_airportcodemapping')")
airportData = spark.sql("SELECT _id, airportName FROM airportTable2 WHERE airportName > 'Moscow' ORDER BY _id")
airportData.printSchema()
print 'Total # of rows in airportData: ' + str(airportData.count())
airportData.show()

spark.sql(" CREATE TEMPORARY VIEW airportTable3 USING org.apache.bahir.cloudant OPTIONS ( database 'n_airportcodemapping')")
airportData = spark.sql("SELECT _id, airportName FROM airportTable3 WHERE airportName > 'Moscow' AND  airportName < 'Sydney'  ORDER BY _id")
airportData.printSchema()
print 'Total # of rows in airportData: ' + str(airportData.count())
airportData.show()

spark.sql(" CREATE TEMPORARY VIEW flight1 USING org.apache.bahir.cloudant OPTIONS ( database 'n_flight')")
flightData = spark.sql("SELECT flightSegmentId, economyClassBaseCost, numFirstClassSeats FROM flight1 WHERE economyClassBaseCost >=200 AND numFirstClassSeats<=10")
flightData.printSchema()
print 'Total # of rows in airportData: ' + str(flightData.count())
flightData.show()

spark.sql(" CREATE TEMPORARY VIEW flight2 USING org.apache.bahir.cloudant OPTIONS ( database 'n_flight')")
flightData = spark.sql("SELECT flightSegmentId, scheduledDepartureTime, scheduledArrivalTime FROM flight2 WHERE scheduledDepartureTime >='2014-12-15T05:00:00.000Z' AND scheduledArrivalTime <='2014-12-15T11:04:00.000Z'")
flightData.printSchema()
print 'Total # of rows in airportData: ' + str(flightData.count())
flightData.show()


