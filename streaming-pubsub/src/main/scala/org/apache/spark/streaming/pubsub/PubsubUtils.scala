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

package org.apache.spark.streaming.pubsub

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object PubsubUtils {

  /**
   * Create an input stream that receives messages pushed by a Pub/Sub publisher
   * using service account authentication
   *
   * @param ssc          StreamingContext object
   * @param project      Google cloud project id
   * @param subscription Subscription name to subscribe to
   * @param credentials  SparkGCPCredentials to use for authenticating
   * @param storageLevel RDD storage level
   * @return
   */
  def createStream(
      ssc: StreamingContext,
      project: String,
      subscription: String,
      credentials: SparkGCPCredentials,
      storageLevel: StorageLevel): ReceiverInputDStream[SparkPubsubMessage] = {
    ssc.withNamedScope("pubsub stream") {

      new PubsubInputDStream(
        ssc,
        project,
        subscription,
        credentials,
        storageLevel)
    }
  }

  /**
   * Create an input stream that receives messages pushed by a Pub/Sub publisher
   * using given credential
   *
   * @param jssc         JavaStreamingContext object
   * @param project      Google cloud project id
   * @param subscription Subscription name to subscribe to
   * @param credentials  SparkGCPCredentials to use for authenticating
   * @param storageLevel RDD storage level
   * @return
   */
  def createStream(jssc: JavaStreamingContext, project: String, subscription: String,
      credentials: SparkGCPCredentials, storageLevel: StorageLevel
      ): JavaReceiverInputDStream[SparkPubsubMessage] = {
    createStream(jssc.ssc, project, subscription, credentials, storageLevel)
  }
}

