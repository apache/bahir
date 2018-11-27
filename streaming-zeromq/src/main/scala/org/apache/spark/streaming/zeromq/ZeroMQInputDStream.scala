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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.zeromq.ZContext
import org.zeromq.ZMQ
import org.zeromq.ZMQException
import org.zeromq.ZMsg

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

/**
 * ZeroMQ receive stream.
 */
private[streaming]
class ZeroMQInputDStream[T: ClassTag](
    ssc: StreamingContext,
    publisherUrl: String,
    connect: Boolean,
    topics: Seq[Array[Byte]],
    bytesToObjects: Array[Array[Byte]] => Iterable[T],
    storageLevel: StorageLevel)
  extends ReceiverInputDStream[T](ssc) with Logging {

  override def getReceiver(): Receiver[T] = {
    new ZeroMQReceiver(publisherUrl, connect, topics, bytesToObjects, storageLevel)
  }
}

private[zeromq]
class ZeroMQReceiver[T: ClassTag](
    publisherUrl: String,
    connect: Boolean,
    topics: Seq[Array[Byte]],
    bytesToObjects: Array[Array[Byte]] => Iterable[T],
    storageLevel: StorageLevel)
  extends Receiver[T](storageLevel) {

  private var receivingThread: Thread = _

  override def onStart(): Unit = {
    receivingThread = new Thread("zeromq-receiver-" + publisherUrl) {
      override def run() {
        subscribe()
      }
    }
    receivingThread.start()
  }

  def subscribe(): Unit = {
    val context = new ZContext

    // JeroMQ requires to create and destroy socket in the same thread.
    // Socket API is not thread-safe.
    val socket = context.createSocket(ZMQ.SUB)
    topics.foreach(socket.subscribe)
    socket.setReceiveTimeOut(1000)
    if (connect) {
      socket.connect(publisherUrl)
    } else {
      socket.bind(publisherUrl)
    }

    try {
      while (!isStopped()) {
        receiveLoop(socket)
      }
    } finally {
      // Context will take care of destructing all associated sockets.
      context.close()
    }
  }

  def receiveLoop(socket: ZMQ.Socket): Unit = {
    try {
      val message = ZMsg.recvMsg(socket)
      if (message != null) {
        val frames = new ArrayBuffer[Array[Byte]]
        message.asScala.foreach(f => frames.append(f.getData))
        bytesToObjects(frames.toArray).foreach(store)
      }
    } catch {
      case e: ZMQException =>
        if (e.getErrorCode != zmq.ZError.ETERM
          && e.getErrorCode != zmq.ZError.EINTR) {
          // 1) Context was terminated. It means that we have just closed the context
          // from a different thread, while trying to receive new message.
          // Error is expected and can happen in normal situation.
          // Reference: http://zguide.zeromq.org/java:interrupt.
          // 2) System call interrupted.
          throw e
        }
    }
  }

  override def onStop(): Unit = {
    receivingThread.join()
  }
}

