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
import scala.language.postfixOps

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import org.apache.spark.{ConditionalSparkFunSuite, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

class PubsubStreamSuite extends ConditionalSparkFunSuite with Eventually with BeforeAndAfter {

  val batchDuration = Seconds(1)

  val blockSize = 15

  private val master: String = "local[2]"

  private val appName: String = this.getClass.getSimpleName

  private val topicName: String = s"bahirStreamTestTopic_${UUID.randomUUID()}"

  private val subscriptionName: String = s"${topicName}_sub"

  private val subForCreateName: String = s"${topicName}_create_me"

  private var ssc: StreamingContext = _
  private var pubsubTestUtils: PubsubTestUtils = _
  private var topicFullName: String = _
  private var subscriptionFullName: String = _
  private var subForCreateFullName: String = _

  override def beforeAll(): Unit = {
    runIf(() => PubsubTestUtils.shouldRunTest()) {
      pubsubTestUtils = new PubsubTestUtils
      topicFullName = pubsubTestUtils.getFullTopicPath(topicName)
      subscriptionFullName = pubsubTestUtils.getFullSubscriptionPath(subscriptionName)
      subForCreateFullName = pubsubTestUtils.getFullSubscriptionPath(subForCreateName)
      pubsubTestUtils.createTopic(topicFullName)
      pubsubTestUtils.createSubscription(topicFullName, subscriptionFullName)
    }
  }

  override def afterAll(): Unit = {
    if (pubsubTestUtils != null) {
      pubsubTestUtils.removeSubscription(subForCreateFullName)
      pubsubTestUtils.removeSubscription(subscriptionFullName)
      pubsubTestUtils.removeTopic(topicFullName)
    }
  }


  def setSparkBackPressureConf(conf: SparkConf) : Unit = {
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.backpressure.initialRate", "50")
    conf.set("spark.streaming.receiver.maxRate", "100")
    conf.set("spark.streaming.backpressure.pid.minRate", "10")
    conf.set("spark.streaming.blockQueueSize", blockSize.toString)
    conf.set("spark.streaming.blockInterval", "1000ms")
  }


  before {
    ssc = new StreamingContext(master, appName, batchDuration)
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }

  test("PubsubUtils API") {
    val pubsubStream1 = PubsubUtils.createStream(
      ssc, "project", None, "subscription",
      PubsubTestUtils.credential, StorageLevel.MEMORY_AND_DISK_SER_2)

    val pubsubStream2 = PubsubUtils.createStream(
      ssc, "project", Some("topic"), "subscription",
      PubsubTestUtils.credential, StorageLevel.MEMORY_AND_DISK_SER_2)
  }

  testIf("pubsub input stream", () => PubsubTestUtils.shouldRunTest()) {
    val receiveStream = PubsubUtils.createStream(
      ssc, PubsubTestUtils.projectId, Some(topicName), subscriptionName,
      PubsubTestUtils.credential, StorageLevel.MEMORY_AND_DISK_SER_2)
    sendReceiveMessages(receiveStream)
  }

  testIf("manual acknowledgement", () => PubsubTestUtils.shouldRunTest()) {
    val receiveStream = PubsubUtils.createStream(
      ssc, PubsubTestUtils.projectId, Some(topicName), subscriptionName,
      PubsubTestUtils.credential, StorageLevel.MEMORY_AND_DISK_SER_2, autoAcknowledge = false)
    sendReceiveMessages(receiveStream)
    ssc.stop()
    assert(pubsubTestUtils.receiveData(subscriptionFullName, 10).nonEmpty)
  }

  testIf("create subscription", () => PubsubTestUtils.shouldRunTest()) {
    val receiveStream = PubsubUtils.createStream(
      ssc, PubsubTestUtils.projectId, Some(topicName), subForCreateName,
      PubsubTestUtils.credential, StorageLevel.MEMORY_AND_DISK_SER_2)
    sendReceiveMessages(receiveStream)
  }

  testIf("check block size", () => PubsubTestUtils.shouldRunTest()) {
    setSparkBackPressureConf(ssc.sparkContext.conf)
    val receiveStream = PubsubUtils.createStream(
      ssc, PubsubTestUtils.projectId, Some(topicName), subForCreateName,
      PubsubTestUtils.credential, StorageLevel.MEMORY_AND_DISK_SER_2, autoAcknowledge = true, 50)

    @volatile var partitionSize: Set[Int] = Set[Int]()
    receiveStream.foreachRDD(rdd => {
      rdd.collectPartitions().foreach(partition => {
        partitionSize += partition.length
      })
    })

    ssc.start()

    eventually(timeout(100000 milliseconds), interval(1000 milliseconds)) {
      pubsubTestUtils.publishData(topicFullName, pubsubTestUtils.generatorMessages(100))
      assert(partitionSize.max == blockSize)
    }
  }

  private def sendReceiveMessages(receiveStream: ReceiverInputDStream[SparkPubsubMessage]): Unit = {
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
      assert(
        sendMessages.map(m => new String(m.getData()))
          .contains(new String(receiveMessages.head.getData()))
      )
      assert(sendMessages.map(_.getAttributes()).contains(receiveMessages.head.getAttributes()))
      assert(receiveMessages.head.getAckId() != null)
    }
  }
}
