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

package org.apache.spark.streaming.twitter

import java.util.UUID

import scala.collection.mutable

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.scalatest.time
import org.scalatest.time.Span
import twitter4j.{FilterQuery, Status, TwitterFactory}
import twitter4j.auth.{Authorization, NullAuthorization}

import org.apache.spark.ConditionalSparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

class TwitterStreamSuite extends ConditionalSparkFunSuite
    with Eventually with BeforeAndAfter with Logging {
  def shouldRunTest(): Boolean = sys.env.get("ENABLE_TWITTER_TESTS").contains("1")

  var ssc: StreamingContext = _

  before {
    ssc = new StreamingContext("local[2]", this.getClass.getSimpleName, Seconds(1))
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }

  test("twitter input stream") {
    val filters = Seq("filter1", "filter2")
    val query = new FilterQuery().language("fr,es")
    val authorization: Authorization = NullAuthorization.getInstance()

    // tests the API, does not actually test data receiving
    val test1: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)
    val test2: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, filters)
    val test3: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_AND_DISK_SER_2)
    val test4: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, Some(authorization))
    val test5: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, Some(authorization), filters)
    val test6: ReceiverInputDStream[Status] = TwitterUtils.createStream(
      ssc, Some(authorization), filters, StorageLevel.MEMORY_AND_DISK_SER_2)
    val test7: ReceiverInputDStream[Status] = TwitterUtils.createFilteredStream(
      ssc, Some(authorization), Some(query), StorageLevel.MEMORY_AND_DISK_SER_2)
  }

  testIf("messages received", () => TwitterStreamSuite.this.shouldRunTest()) {
    val userId = TwitterFactory.getSingleton.updateStatus(
      UUID.randomUUID().toString
    ).getUser.getId

    val receiveStream = TwitterUtils.createFilteredStream(
      ssc, None, Some(new FilterQuery().follow(userId))
    )
    @volatile var receivedMessages: mutable.Set[Status] = mutable.Set()
    receiveStream.foreachRDD { rdd =>
      for (element <- rdd.collect()) {
        receivedMessages += element
      }
      receivedMessages
    }
    ssc.start()

    val nbOfMsg = 2
    var publishedMessages: List[String] = List()

    (1 to nbOfMsg).foreach(
      _ => {
        publishedMessages = UUID.randomUUID().toString :: publishedMessages
      }
    )

    eventually(timeout(Span(15, time.Seconds)), interval(Span(1000, time.Millis))) {
      publishedMessages.foreach(
        m => if (!receivedMessages.map(m => m.getText).contains(m.toString)) {
          TwitterFactory.getSingleton.updateStatus(m)
        }
      )
      assert(
        publishedMessages.map(m => m.toString).toSet
          .subsetOf(receivedMessages.map(m => m.getText))
      )
    }
  }
}
