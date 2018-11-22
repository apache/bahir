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

package org.apache.spark.streaming.pubnub

import java.util.{Set => JSet}

import collection.JavaConverters._
import com.pubnub.api.PNConfiguration

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object PubNubUtils {
  /**
   * Create an input stream that returns messages received from PubNub infrastructure.
   * @param ssc Streaming context
   * @param configuration PubNub client configuration
   * @param channels Sequence of channels to subscribe
   * @param channelGroups Sequence of channel groups to subscribe
   * @param timeToken Optional point in time to start receiving messages from.
   *                  Leave undefined to get only latest messages.
   * @param storageLevel Storage level to use for storing the received objects
   * @return Input stream
   */
  def createStream(
      ssc: StreamingContext,
      configuration: PNConfiguration,
      channels: Seq[String],
      channelGroups: Seq[String],
      timeToken: Option[Long] = None,
      storageLevel: StorageLevel): ReceiverInputDStream[SparkPubNubMessage] = {
    ssc.withNamedScope("PubNub Stream") {
      new PubNubInputDStream(
        ssc, configuration, channels, channelGroups, timeToken, storageLevel
      )
    }
  }

  /**
   * Create an input stream that returns messages received from PubNub infrastructure.
   * @param jssc Java streaming context
   * @param configuration PubNub client configuration
   * @param channels Set of channels to subscribe
   * @param channelGroups Set of channel groups to subscribe
   * @param timeToken Optional point in time to start receiving messages from.
   *                  Specify <code>null</code> to get only latest messages.
   * @param storageLevel Storage level to use for storing the received objects
   * @return Input stream
   */
  def createStream(
      jssc: JavaStreamingContext,
      configuration: PNConfiguration,
      channels: JSet[String],
      channelGroups: JSet[String],
      timeToken: Option[Long],
      storageLevel: StorageLevel): JavaReceiverInputDStream[SparkPubNubMessage] = {
    createStream(
      jssc.ssc, configuration, Seq.empty ++ channels.asScala,
      Seq.empty ++ channelGroups.asScala, timeToken, storageLevel
    )
  }
}
