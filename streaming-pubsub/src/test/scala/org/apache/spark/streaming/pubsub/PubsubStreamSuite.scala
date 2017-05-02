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

import java.util.UUID

import scala.concurrent.duration._

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds

class PubsubStreamSuite extends PubsubFunSuite with Eventually with BeforeAndAfter {

  val batchDuration = Seconds(1)

  private val master: String = "local[2]"

  private val appName: String = this.getClass.getSimpleName

  private val topicName: String = s"bahirStreamTestTopic_${UUID.randomUUID()}"

  private val subscriptionName: String = s"${topicName}_sub"

  private var ssc: StreamingContext = null
  private var pubsubTestUtils: PubsubTestUtils = null
  private var topicFullName: String = null
  private var subscriptionFullName: String = null

  override def beforeAll(): Unit = {
    runIfTestsEnabled("Prepare PubsubTestUtils") {
      pubsubTestUtils = new PubsubTestUtils
      topicFullName = pubsubTestUtils.getFullTopicPath(topicName)
      subscriptionFullName = pubsubTestUtils.getFullSubscriptionPath(subscriptionName)
    }
  }

  before {
    ssc = new StreamingContext(master, appName, batchDuration)
    if (pubsubTestUtils != null) {
      pubsubTestUtils.createTopic(topicFullName)
      pubsubTestUtils.createSubscription(topicFullName, subscriptionFullName)
    }
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
    if (pubsubTestUtils != null) {
      pubsubTestUtils.removeSubscription(subscriptionFullName)
      pubsubTestUtils.removeTopic(topicFullName)
    }
  }

  test("PubsubUtils API") {
    val pubsubStream = PubsubUtils.createStream(
      ssc, "project", "subscription",
      SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
  }

  testIfEnabled("pubsub input stream") {
    val receiveStream = PubsubUtils.createStream(ssc, PubsubTestUtils.projectId, subscriptionName,
      PubsubTestUtils.credential, StorageLevel.MEMORY_AND_DISK_SER_2)

    @volatile var receiveMessages: List[SparkPubsubMessage] = List()
    receiveStream.foreachRDD { rdd =>
      if (rdd.collect().length > 0) {
        receiveMessages = receiveMessages ::: List(rdd.first)
        receiveMessages
      }
    }

    ssc.start()

    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      val sendMessages = pubsubTestUtils.generatorMessages(10)
      pubsubTestUtils.publishData(topicFullName, sendMessages)
      assert(sendMessages.map(m => new String(m.getData))
          .contains(new String(receiveMessages(0).getData)))
      assert(sendMessages.map(_.getAttributes).contains(receiveMessages(0).getAttributes))
    }

    ssc.stop()
  }
}
