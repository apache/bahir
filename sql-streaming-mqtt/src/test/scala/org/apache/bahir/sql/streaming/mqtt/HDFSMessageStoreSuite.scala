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

package org.apache.bahir.sql.streaming.mqtt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.eclipse.paho.client.mqttv3.MqttClientPersistence
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SharedSparkContext, SparkFunSuite}


class HDFSMessageStoreSuite extends SparkFunSuite with SharedSparkContext with BeforeAndAfter {

  private val testData = Seq(1, 2, 3, 4, 5, 6)
  private val javaSerializer: JavaSerializer = new JavaSerializer()
  private val serializerInstance = javaSerializer
  private var config: Configuration = _
  private var hadoop: MiniDFSCluster = _
  private var persistence: MqttClientPersistence = _
  private var store: LocalMessageStore = _

  override def beforeAll() {
    val (hadoopConfig, hadoopInstance) = HDFSTestUtils.prepareHadoop()
    config = hadoopConfig
    hadoop = hadoopInstance
    persistence = new HdfsMqttClientPersistence(config)
    persistence.open("temp", "tcp://dummy-url:0000")
    store = new LocalMessageStore(persistence, javaSerializer)
  }

  override def afterAll() {
    store = null
    persistence.clear()
    persistence.close()
    persistence = null
    if (hadoop != null) {
      hadoop.shutdown(true)
    }
    hadoop = null
    config = null
  }

  test("serialize and deserialize") {
    val serialized = serializerInstance.serialize(testData)
    val deserialized: Seq[Int] = serializerInstance
      .deserialize(serialized).asInstanceOf[Seq[Int]]
    assert(testData === deserialized)
  }

  test("Store, retrieve and remove") {
    store.store(1, testData)
    var result: Seq[Int] = store.retrieve(1)
    assert(testData === result)
    store.remove(1)
  }

  test("Max offset stored") {
    store.store(1, testData)
    store.store(10, testData)
    val offset = store.maxProcessedOffset
    assert(offset == 10)
  }

}
