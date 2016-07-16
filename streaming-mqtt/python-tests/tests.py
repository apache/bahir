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
from mqtt import MQTTUtils


class PySparkStreamingTestCase(unittest.TestCase):

    timeout = 10  # seconds
    duration = .5

    @classmethod
    def setUpClass(cls):
        class_name = cls.__name__
        conf = SparkConf().set("spark.default.parallelism", 1)
        cls.sc = SparkContext(appName=class_name, conf=conf)
        cls.sc.setCheckpointDir("/tmp")

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
        # Clean up in the JVM just in case there has been some issues in Python API
        try:
            jSparkContextOption = SparkContext._jvm.SparkContext.get()
            if jSparkContextOption.nonEmpty():
                jSparkContextOption.get().stop()
        except:
            pass

    def setUp(self):
        self.ssc = StreamingContext(self.sc, self.duration)

    def tearDown(self):
        if self.ssc is not None:
            self.ssc.stop(False)
        # Clean up in the JVM just in case there has been some issues in Python API
        try:
            jStreamingContextOption = StreamingContext._jvm.SparkContext.getActive()
            if jStreamingContextOption.nonEmpty():
                jStreamingContextOption.get().stop(False)
        except:
            pass

    def wait_for(self, result, n):
        start_time = time.time()
        while len(result) < n and time.time() - start_time < self.timeout:
            time.sleep(0.01)
        if len(result) < n:
            print("timeout after", self.timeout)

    def _take(self, dstream, n):
        """
        Return the first `n` elements in the stream (will start and stop).
        """
        results = []

        def take(_, rdd):
            if rdd and len(results) < n:
                results.extend(rdd.take(n - len(results)))

        dstream.foreachRDD(take)

        self.ssc.start()
        self.wait_for(results, n)
        return results

    def _collect(self, dstream, n, block=True):
        """
        Collect each RDDs into the returned list.

        :return: list, which will have the collected items.
        """
        result = []

        def get_output(_, rdd):
            if rdd and len(result) < n:
                r = rdd.collect()
                if r:
                    result.append(r)

        dstream.foreachRDD(get_output)

        if not block:
            return result

        self.ssc.start()
        self.wait_for(result, n)
        return result

    def _test_func(self, input, func, expected, sort=False, input2=None):
        """
        @param input: dataset for the test. This should be list of lists.
        @param func: wrapped function. This function should return PythonDStream object.
        @param expected: expected output for this testcase.
        """
        if not isinstance(input[0], RDD):
            input = [self.sc.parallelize(d, 1) for d in input]
        input_stream = self.ssc.queueStream(input)
        if input2 and not isinstance(input2[0], RDD):
            input2 = [self.sc.parallelize(d, 1) for d in input2]
        input_stream2 = self.ssc.queueStream(input2) if input2 is not None else None

        # Apply test function to stream.
        if input2:
            stream = func(input_stream, input_stream2)
        else:
            stream = func(input_stream)

        result = self._collect(stream, len(expected))
        if sort:
            self._sort_result_based_on_key(result)
            self._sort_result_based_on_key(expected)
        self.assertEqual(expected, result)

    def _sort_result_based_on_key(self, outputs):
        """Sort the list based on first value."""
        for output in outputs:
            output.sort(key=lambda x: x[0])


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
