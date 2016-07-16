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
#

"""
 A sample wordcount with MqttStream stream
 Usage: mqtt_wordcount.py <broker url> <topic>

 To run this in your local machine, you need to setup a MQTT broker and publisher first,
 like Mosquitto (http://mosquitto.org/) an easy to use and install open source MQTT Broker.
 On Mac OS Mosquitto can be installed with Homebrew `$ brew install mosquitto`.
 On Ubuntu mosquitto can be installed with the command `$ sudo apt-get install mosquitto`.

 Alternatively, the Eclipse paho project provides a number of clients and utilities for
 working with MQTT, see http://www.eclipse.org/paho/#getting-started

 How to run this example locally:

 (1) Start Mqtt message broker/server, i.e. Mosquitto:

    `$ mosquitto -p 1883`

 (2) Run the publisher:

    `$ bin/run-example \
      org.apache.spark.examples.streaming.mqtt.MQTTPublisher tcp://localhost:1883 foo`

 (3) Run the example:

    `$ bin/run-example \
      streaming-mqtt/examples/src/main/python/streaming/mqtt_wordcount.py tcp://localhost:1883 foo`
"""

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: mqtt_wordcount.py <broker url> <topic>"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingMQTTWordCount")
    ssc = StreamingContext(sc, 1)

    brokerUrl = sys.argv[1]
    topic = sys.argv[2]

    lines = MQTTUtils.createStream(ssc, brokerUrl, topic)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
