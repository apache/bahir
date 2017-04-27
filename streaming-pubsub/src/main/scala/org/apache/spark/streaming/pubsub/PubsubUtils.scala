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

import com.google.api.services.pubsub.PubsubScopes
import com.google.cloud.hadoop.util.{EntriesCredentialConfiguration, HadoopCredentialConfiguration}
import java.util
import org.apache.hadoop.conf.Configuration

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object PubsubUtils {

  val PUBSUB_PREFIX = "sparkstreaming.pubsub"

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
        HadoopCredentialConfiguration
            .newBuilder()
            .withConfiguration(new Configuration(false))
            .withOverridePrefix(PUBSUB_PREFIX)
            .build()
            .getCredential(new util.ArrayList(PubsubScopes.all())),
        storageLevel)
    }
  }

  def createStream(
      ssc: StreamingContext,
      project: String,
      subscription: String,
      serviceAccountJsonFilePath: String,
      storageLevel: StorageLevel): ReceiverInputDStream[SparkPubsubMessage] = {
    ssc.withNamedScope("pubsub stream") {
      val conf = new Configuration(false)
      conf.setBoolean(
        PUBSUB_PREFIX + EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
        true)
      conf.set(
        PUBSUB_PREFIX + EntriesCredentialConfiguration.JSON_KEYFILE_SUFFIX,
        serviceAccountJsonFilePath
      )
      new PubsubInputDStream(
        ssc,
        project,
        subscription,
        HadoopCredentialConfiguration
            .newBuilder()
            .withConfiguration(conf)
            .withOverridePrefix(PUBSUB_PREFIX)
            .build()
            .getCredential(new util.ArrayList(PubsubScopes.all())),
        storageLevel)
    }
  }

  def createStream(
      ssc: StreamingContext,
      project: String,
      subscription: String,
      serviceAccountP12Email: String,
      serviceAccountP12FilePath: String,
      storageLevel: StorageLevel): ReceiverInputDStream[SparkPubsubMessage] = {
    ssc.withNamedScope("pubsub stream") {
      val conf = new Configuration(false)
      conf.setBoolean(
        PUBSUB_PREFIX + EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
        true)
      conf.set(
        PUBSUB_PREFIX + EntriesCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX,
        serviceAccountP12Email
      )
      conf.set(
        PUBSUB_PREFIX + EntriesCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX,
        serviceAccountP12FilePath
      )
      new PubsubInputDStream(
        ssc,
        project,
        subscription,
        HadoopCredentialConfiguration
            .newBuilder()
            .withConfiguration(conf)
            .withOverridePrefix(PUBSUB_PREFIX)
            .build()
            .getCredential(new util.ArrayList(PubsubScopes.all())),
        storageLevel)
    }
  }

}
