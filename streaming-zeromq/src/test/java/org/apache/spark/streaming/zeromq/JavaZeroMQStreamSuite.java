/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.zeromq;

import org.junit.Test;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import zmq.ZMQ;

import java.util.Arrays;

public class JavaZeroMQStreamSuite extends LocalJavaStreamingContext {
    @Test
    public void testZeroMQAPICompatibility() {
        // Test the API, but do not exchange any messages.
        final String publishUrl = "tcp://localhost:5555";
        final String topic = "topic1";
        final Function<byte[][], Iterable<String>> messageConverter =
                new Function<byte[][], Iterable<String>>() {
                    @Override
                    public Iterable<String> call(byte[][] bytes) throws Exception {
                        // Skip topic name and assume that each message contains only one frame.
                        return Arrays.asList(new String(bytes[1], ZMQ.CHARSET));
                    }
                };

        JavaReceiverInputDStream<String> test1 = ZeroMQUtils.createJavaStream(
                ssc, publishUrl, true, Arrays.asList(topic.getBytes()), messageConverter,
                StorageLevel.MEMORY_AND_DISK_SER_2()
        );
        JavaReceiverInputDStream<String> test2 = ZeroMQUtils.createTextJavaStream(
                ssc, publishUrl, true, Arrays.asList(topic.getBytes()),
                StorageLevel.MEMORY_AND_DISK_SER_2()
        );
    }
}

