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

import sys
import time
import random

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark.context import SparkConf, SparkContext, RDD
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.tests import PySparkStreamingTestCase
from mqtt import MQTTUtils

class MQTTStreamTests(PySparkStreamingTestCase):
    timeout = 20  # seconds
    duration = 1

    def setUp(self):
        super(MQTTStreamTests, self).setUp()

        MQTTTestUtilsClz = self.ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
            .loadClass("org.apache.spark.streaming.mqtt.MQTTTestUtils")
        self._MQTTTestUtils = MQTTTestUtilsClz.newInstance()
        self._MQTTTestUtils.setup()

    def tearDown(self):
        if self._MQTTTestUtils is not None:
            self._MQTTTestUtils.teardown()
            self._MQTTTestUtils = None

        super(MQTTStreamTests, self).tearDown()

    def _randomTopic(self):
        return "topic-%d" % random.randint(0, 10000)

    def _startContext(self, topic):
        # Start the StreamingContext and also collect the result
        stream = MQTTUtils.createStream(self.ssc, "tcp://" + self._MQTTTestUtils.brokerUri(), topic)
        result = []

        def getOutput(_, rdd):
            for data in rdd.collect():
                result.append(data)

        stream.foreachRDD(getOutput)
        self.ssc.start()
        return result

    def test_mqtt_stream(self):
        """Test the Python MQTT stream API."""
        sendData = "MQTT demo for spark streaming"
        topic = self._randomTopic()
        result = self._startContext(topic)

        def retry():
            self._MQTTTestUtils.publishData(topic, sendData)
            # Because "publishData" sends duplicate messages, here we should use > 0
            self.assertTrue(len(result) > 0)
            self.assertEqual(sendData, result[0])

        # Retry it because we don't know when the receiver will start.
        self._retry_or_timeout(retry)

    def _retry_or_timeout(self, test_func):
        start_time = time.time()
        while True:
            try:
                test_func()
                break
            except:
                if time.time() - start_time > self.timeout:
                    raise
                time.sleep(0.01)


if __name__ == "__main__":
    unittest.main()
