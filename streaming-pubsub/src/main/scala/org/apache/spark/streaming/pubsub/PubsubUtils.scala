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
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.pubsub.ServiceAccountType.ServiceAccountType

object PubsubUtils {

  /**
   * Create an input stream that receives messages pushed by a Pub/Sub publisher.
   * @param ssc           StreamingContext object
   * @param project       Google cloud project id
   * @param subscription  Subscription name to subscribe to
   * @param storageLevel  RDD storage level
   */
  def createStream(
      ssc: StreamingContext,
      project: String,
      subscription: String,
      storageLevel: StorageLevel): ReceiverInputDStream[SparkPubsubMessage] = {
    ssc.withNamedScope("pubsub stream") {

      new PubsubInputDStream(
        ssc,
        project,
        subscription,
        ApplicationDefaultCredentials,
        storageLevel)
    }
  }

  /**
   * Create an input stream that receives messages pushed by a Pub/Sub publisher.
   * @param ssc                     StreamingContext object
   * @param project                 Google cloud project id
   * @param subscription            Subscription name to subscribe to
   * @param serviceAccountType      Type of service account: Metadata, Json, P12
   * @param serviceAccountJsonPath  File path for Json type of service account
   * @param serviceAccountEmail     Email account of service account
   * @param serviceAccountP12Path   File path for P12 type of service account
   * @param storageLevel            RDD storage level
   * @return
   */
  def createStream(
      ssc: StreamingContext,
      project: String,
      subscription: String,
      serviceAccountType: ServiceAccountType,
      serviceAccountJsonPath: String,
      serviceAccountEmail: String,
      serviceAccountP12Path: String,
      storageLevel: StorageLevel): ReceiverInputDStream[SparkPubsubMessage] = {
    ssc.withNamedScope("pubsub stream") {

      val serviceAccountCredentials = serviceAccountType match {
        case ServiceAccountType.Metadata => new ServiceAccountCredentials()
        case ServiceAccountType.Json =>
          new ServiceAccountCredentials(Option(serviceAccountJsonPath))
        case ServiceAccountType.P12 =>
          new ServiceAccountCredentials(jsonFilePath = Option(serviceAccountP12Path),
            emailAccount = Option(serviceAccountEmail))
      }

      new PubsubInputDStream(
        ssc,
        project,
        subscription,
        serviceAccountCredentials,
        storageLevel)
    }
  }
}

object ServiceAccountType extends Enumeration {
  type ServiceAccountType = Value
  val Metadata, Json, P12 = Value
}
