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

import scala.collection.JavaConverters._

import com.google.api.services.pubsub.Pubsub
import com.google.api.services.pubsub.Pubsub.Builder
import com.google.api.services.pubsub.model.PublishRequest
import com.google.api.services.pubsub.model.PubsubMessage
import com.google.api.services.pubsub.model.Subscription
import com.google.api.services.pubsub.model.Topic
import com.google.cloud.hadoop.util.RetryHttpInitializer

import org.apache.spark.internal.Logging

private[pubsub] class PubsubTestUtils extends Logging {

  val APP_NAME = this.getClass.getSimpleName

  val client: Pubsub = {
    new Builder(
      ConnectionUtils.transport,
      ConnectionUtils.jacksonFactory,
      new RetryHttpInitializer(
        PubsubTestUtils.credential.provider,
        APP_NAME
      ))
        .setApplicationName(APP_NAME)
        .build()
  }

  def createTopic(topic: String): Unit = {
    val topicRequest = new Topic()
    client.projects().topics().create(topic, topicRequest.setName(topic)).execute()
  }

  def createSubscription(topic: String, subscription: String): Unit = {
    val subscriptionRequest = new Subscription()
    client.projects().subscriptions().create(subscription,
      subscriptionRequest.setTopic(topic).setName(subscription)).execute()
  }

  def publishData(topic: String, messages: List[SparkPubsubMessage]): Unit = {
    val publishRequest = new PublishRequest()
    publishRequest.setMessages(messages.map(m => m.message).asJava)
    client.projects().topics().publish(topic, publishRequest).execute()
  }

  def removeSubscription(subscription: String): Unit = {
    client.projects().subscriptions().delete(subscription).execute()
  }

  def removeTopic(topic: String): Unit = {
    client.projects().topics().delete(topic).execute()
  }

  def generatorMessages(num: Int): List[SparkPubsubMessage] = {
    (1 to num)
        .map(n => {
          val m = new PubsubMessage()
          m.encodeData(s"data$n".getBytes)
          m.setAttributes(Map("a1" -> s"v1$n", "a2" -> s"v2$n").asJava)
        })
        .map(m => {
          val sm = new SparkPubsubMessage()
          sm.message = m
          sm
        })
        .toList
  }

  def getFullTopicPath(topic: String): String =
    s"projects/${PubsubTestUtils.projectId}/topics/$topic"

  def getFullSubscriptionPath(subscription: String): String =
    s"projects/${PubsubTestUtils.projectId}/subscriptions/$subscription"

}

private[pubsub] object PubsubTestUtils {

  val envVarNameForEnablingTests = "ENABLE_PUBSUB_TESTS"
  val envVarNameForGoogleCloudProjectId = "GCP_TEST_PROJECT_ID"
  val envVarNameForJsonKeyPath = "GCP_TEST_JSON_KEY_PATH"
  val envVarNameForP12KeyPath = "GCP_TEST_P12_KEY_PATH"
  val envVarNameForAccount = "GCP_TEST_ACCOUNT"

  lazy val shouldRunTests = {
    val isEnvSet = sys.env.get(envVarNameForEnablingTests) == Some("1")
    if (isEnvSet) {
      // scalastyle:off println
      // Print this so that they are easily visible on the console and not hidden in the log4j logs.
      println(
        s"""
           |Google Pub/Sub tests that actually send data has been enabled by setting the environment
           |variable $envVarNameForEnablingTests to 1.
           |This will create Pub/Sub Topics and Subscriptions in Google cloud platform.
           |Please be aware that this may incur some Google cloud costs.
           |Set the environment variable $envVarNameForGoogleCloudProjectId to the desired project.
        """.stripMargin)
      // scalastyle:on println
    }
    isEnvSet
  }

  lazy val projectId = {
    val id = sys.env.getOrElse(envVarNameForGoogleCloudProjectId,
      throw new IllegalArgumentException(
        s"Need to set environment varibable $envVarNameForGoogleCloudProjectId if enable test."))
    // scalastyle:off println
    // Print this so that they are easily visible on the console and not hidden in the log4j logs.
    println(s"Using project $id for creating Pub/Sub topic and subscription for tests.")
    // scalastyle:on println
    id
  }

  lazy val credential =
    sys.env.get(envVarNameForJsonKeyPath)
        .map(path => SparkGCPCredentials.builder.jsonServiceAccount(path).build())
        .getOrElse(
          sys.env.get(envVarNameForP12KeyPath)
            .map(path => SparkGCPCredentials.builder.p12ServiceAccount(
              path, sys.env.get(envVarNameForAccount).get
            ).build())
            .getOrElse(SparkGCPCredentials.builder.build()))
}
