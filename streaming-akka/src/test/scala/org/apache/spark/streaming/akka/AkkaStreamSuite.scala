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

package org.apache.spark.streaming.akka

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import akka.actor._
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class AkkaStreamSuite extends SparkFunSuite with Eventually with BeforeAndAfter {

  private var ssc: StreamingContext = _

  private var actorSystem: ActorSystem = _

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    if (actorSystem != null) {
      actorSystem.shutdown()
      actorSystem.awaitTermination(30.seconds)
      actorSystem = null
    }
  }

  test("actor input stream") {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
    ssc = new StreamingContext(sparkConf, Milliseconds(500))

    // we set the TCP port to "0" to have the port chosen automatically for the Feeder actor and
    // the Receiver actor will "pick it up" from the Feeder URI when it subscribes to the Feeder
    // actor (http://doc.akka.io/docs/akka/2.3.11/scala/remoting.html)
    val akkaConf = ConfigFactory.parseMap(
      Map(
        "akka.actor.provider" -> "akka.remote.RemoteActorRefProvider",
        "akka.remote.netty.tcp.transport-class" -> "akka.remote.transport.netty.NettyTransport",
        "akka.remote.netty.tcp.port" -> "0").
        asJava)
    actorSystem = ActorSystem("test", akkaConf)
    actorSystem.actorOf(Props(classOf[FeederActor]), "FeederActor")
    val feederUri =
      actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress + "/user/FeederActor"

    val actorStream =
      AkkaUtils.createStream[String](ssc, Props(classOf[TestActorReceiver], feederUri),
        "TestActorReceiver")
    val result = new ConcurrentLinkedQueue[String]
    actorStream.foreachRDD { rdd =>
      rdd.collect().foreach(result.add)
    }
    ssc.start()

    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      assert((1 to 10).map(_.toString) === result.asScala.toList)
    }
  }
}

case class SubscribeReceiver(receiverActor: ActorRef)

class FeederActor extends Actor {

  def receive: Receive = {
    case SubscribeReceiver(receiverActor: ActorRef) =>
      (1 to 10).foreach(i => receiverActor ! i.toString())
  }
}

class TestActorReceiver(uriOfPublisher: String) extends ActorReceiver {

  lazy private val remotePublisher = context.actorSelection(uriOfPublisher)

  override def preStart(): Unit = {
    remotePublisher ! SubscribeReceiver(self)
  }

  def receive: PartialFunction[Any, Unit] = {
    case msg: String => store(msg)
  }

}
