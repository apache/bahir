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

package org.apache.spark.streaming.pubnub;

import com.pubnub.api.PNConfiguration;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.junit.Test;

import java.util.HashSet;

public class JavaPubNubStreamSuite extends LocalJavaStreamingContext {
    @Test
    public void testPubNubStream() {
        // Tests the API compatibility, but do not actually receive any data.
        JavaReceiverInputDStream<SparkPubNubMessage> stream = PubNubUtils.createStream(
                ssc, new PNConfiguration(), new HashSet<>(), new HashSet<>(), null,
                StorageLevel.MEMORY_AND_DISK_SER_2()
        );
    }
}
