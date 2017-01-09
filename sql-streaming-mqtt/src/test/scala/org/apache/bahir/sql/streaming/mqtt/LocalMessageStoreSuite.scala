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

import java.io.File

import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.JavaSerializer

import org.apache.bahir.utils.BahirUtils


class LocalMessageStoreSuite extends SparkFunSuite with BeforeAndAfter {

  private val testData = Seq(1, 2, 3, 4, 5, 6)
  private val javaSerializer: JavaSerializer = new JavaSerializer(new SparkConf())

  private val serializerInstance = javaSerializer.newInstance()
  private val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/mqtt-test2/")
  private val persistence: MqttDefaultFilePersistence =
    new MqttDefaultFilePersistence(tempDir.getAbsolutePath)

  private val store = new LocalMessageStore(persistence, javaSerializer)

  before {
    tempDir.mkdirs()
    tempDir.deleteOnExit()
    persistence.open("temp", "tcp://dummy-url:0000")
  }

  after {
    persistence.clear()
    persistence.close()
    BahirUtils.recursiveDeleteDir(tempDir)
  }

  test("serialize and deserialize") {
      val serialized = serializerInstance.serialize(testData)
    val deserialized: Seq[Int] = serializerInstance
      .deserialize(serialized).asInstanceOf[Seq[Int]]
    assert(testData === deserialized)
  }

  test("Store and retreive") {
    store.store(1, testData)
    val result: Seq[Int] = store.retrieve(1)
    assert(testData === result)
  }

  test("Max offset stored") {
    store.store(1, testData)
    store.store(10, testData)
    val offset: Int = store.maxProcessedOffset
    assert(offset == 10)
  }

}
