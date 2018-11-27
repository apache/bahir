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

package org.apache.spark.streaming.zeromq

import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.zeromq.ZMQ

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object ZeroMQUtils {
  val textMessageConverter: Array[Array[Byte]] => Iterable[String] =
      (bytes: Array[Array[Byte]]) => {
    // First frame typically contains topic name, so we skip it to extract only payload.
    val result = new ArrayBuffer[String]()
    for (i <- 1 until bytes.length) {
      result.append(new String(bytes(i), ZMQ.CHARSET))
    }
    result
  }

  /**
   * Create an input stream that receives messages pushed by a ZeroMQ publisher.
   * @param ssc Streaming context
   * @param publisherUrl URL of remote ZeroMQ publisher
   * @param connect When positive, connector will try to establish connectivity with remote server.
   *                Otherwise, it attempts to create and bind local socket.
   * @param topics List of topics to subscribe
   * @param messageConverter ZeroMQ stream publishes sequence of frames for each topic
   *                         and each frame has sequence of byte thus it needs the converter
   *                         (which might be deserializer of bytes) to translate from sequence
   *                         of sequence of bytes, where sequence refer to a frame
   *                         and sub sequence refer to its payload. First frame typically
   *                         contains message envelope, which corresponds to topic name.
   * @param storageLevel RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createStream[T: ClassTag](
      ssc: StreamingContext,
      publisherUrl: String,
      connect: Boolean,
      topics: Seq[Array[Byte]],
      messageConverter: Array[Array[Byte]] => Iterable[T],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[T] = {
    ssc.withNamedScope("ZeroMQ stream") {
      new ZeroMQInputDStream(ssc, publisherUrl, connect, topics, messageConverter, storageLevel)
    }
  }

  /**
   * Create text input stream that receives messages pushed by a ZeroMQ publisher.
   * @param ssc Streaming context
   * @param publisherUrl URL of remote ZeroMQ publisher
   * @param connect When positive, connector will try to establish connectivity with remote server.
   *                Otherwise, it attempts to create and bind local socket.
   * @param topics List of topics to subscribe
   * @param storageLevel RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createTextStream(
      ssc: StreamingContext,
      publisherUrl: String,
      connect: Boolean,
      topics: Seq[Array[Byte]],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String] = {
    createStream[String](ssc, publisherUrl, connect, topics, textMessageConverter, storageLevel)
  }

  /**
   * Create an input stream that receives messages pushed by a ZeroMQ publisher.
   * @param jssc Java streaming context
   * @param publisherUrl URL of remote ZeroMQ publisher
   * @param connect When positive, connector will try to establish connectivity with remote server.
   *                Otherwise, it attempts to create and bind local socket.
   * @param topics List of topics to subscribe
   * @param messageConverter ZeroMQ stream publishes sequence of frames for each topic and each
   *                         frame has sequence of byte thus it needs the converter (which might be
   *                         deserializer of bytes) to translate from sequence of sequence of bytes,
   *                         where sequence refer to a frame and sub sequence refer to its payload.
   *                         First frame typically contains message envelope, which corresponds
   *                         to topic name.
   * @param storageLevel Storage level to use for persisting received objects
   */
  def createJavaStream[T](
      jssc: JavaStreamingContext,
      publisherUrl: String,
      connect: Boolean,
      topics: JList[Array[Byte]],
      messageConverter: JFunction[Array[Array[Byte]], JIterable[T]],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val fn = (x: Array[Array[Byte]]) =>
      messageConverter.call(x).iterator().asScala.toIterable
    createStream(jssc.ssc, publisherUrl, connect, topics.asScala, fn, storageLevel)
  }

  /**
   * Create text input stream that receives messages pushed by a ZeroMQ publisher.
   * @param jssc Java streaming context
   * @param publisherUrl URL of remote ZeroMQ publisher
   * @param connect When positive, connector will try to establish connectivity with remote server.
   *                Otherwise, it attempts to create and bind local socket.
   * @param topics List of topics to subscribe
   * @param storageLevel Storage level to use for persisting received objects
   */
  def createTextJavaStream(
      jssc: JavaStreamingContext,
      publisherUrl: String,
      connect: Boolean,
      topics: JList[Array[Byte]],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[String] = {
    createStream(jssc.ssc, publisherUrl, connect, topics.asScala,
      textMessageConverter, storageLevel
    )
  }
}
