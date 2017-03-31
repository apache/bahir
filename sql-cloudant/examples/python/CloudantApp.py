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
    .appName("Cloudant Spark SQL Example in Python using temp tables")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .getOrCreate()


# ***1. Loading temp table from Cloudant db
spark.sql(" CREATE TEMPORARY TABLE airportTable USING org.apache.bahir.cloudant OPTIONS ( database 'n_airportcodemapping')")
airportData = spark.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
airportData.printSchema()
print 'Total # of rows in airportData: ' + str(airportData.count())
for code in airportData.collect():
    print code._id


# ***2. Loading temp table from Cloudant search index
print 'About to test org.apache.bahir.cloudant for flight with index'
spark.sql(" CREATE TEMPORARY TABLE flightTable1 USING org.apache.bahir.cloudant OPTIONS ( database 'n_flight', index '_design/view/_search/n_flights')")
flightData = spark.sql("SELECT flightSegmentId, scheduledDepartureTime FROM flightTable1 WHERE flightSegmentId >'AA9' AND flightSegmentId<'AA95'")
flightData.printSchema()
for code in flightData.collect():
    print 'Flight {0} on {1}'.format(code.flightSegmentId, code.scheduledDepartureTime)

