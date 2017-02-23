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

// scalastyle:off println
package org.apache.spark.examples.streaming.akka

import scala.collection.mutable
import scala.util.Random

import akka.actor.{Props, _}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.akka.{ActorReceiver, AkkaUtils}

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

/**
 * Sends the random content to every receiver subscribed with 1/2
 *  second delay.
 */
class FeederActor extends Actor {

  val rand = new Random()
  val receivers = new mutable.LinkedHashSet[ActorRef]()

  val strings: Array[String] = Array("words ", "may ", "count ")

  def makeMessage(): String = {
    val x = rand.nextInt(3)
    strings(x) + strings(2 - x)
  }

  /*
   * A thread to generate random messages
   */
  new Thread() {
    override def run() {
      while (true) {
        Thread.sleep(500)
        receivers.foreach(_ ! makeMessage)
      }
    }
  }.start()

  def receive: Receive = {
    case SubscribeReceiver(receiverActor: ActorRef) =>
      println(s"received subscribe from ${receiverActor.toString}")
      receivers += receiverActor

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      println(s"received unsubscribe from ${receiverActor.toString}")
      receivers -= receiverActor
  }
}

/**
 * A sample actor as receiver, is also simplest. This receiver actor
 * goes and subscribe to a typical publisher/feeder actor and receives
 * data.
 *
 * @see [[org.apache.spark.examples.streaming.akka.FeederActor]]
 */
class SampleActorReceiver[T](urlOfPublisher: String) extends ActorReceiver {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart(): Unit = remotePublisher ! SubscribeReceiver(context.self)

  def receive: PartialFunction[Any, Unit] = {
    case msg => store(msg.asInstanceOf[T])
  }

  override def postStop(): Unit = remotePublisher ! UnsubscribeReceiver(context.self)

}

/**
 * A sample feeder actor
 *
 * Usage: FeederActor <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder would start on.
 */
object FeederActor {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: FeederActor <hostname> <port>\n")
      System.exit(1)
    }
    val Seq(host, port) = args.toSeq

    val akkaConf = ConfigFactory.parseString(
      s"""akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = $port
         |""".stripMargin)
       val actorSystem = ActorSystem("test", akkaConf)
    val feeder = actorSystem.actorOf(Props[FeederActor], "FeederActor")

    println("Feeder started as:" + feeder)

    actorSystem.awaitTermination()
  }
}

/**
 * A sample word count program demonstrating the use of plugging in
 *
 * Actor as Receiver
 * Usage: ActorWordCount <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder is running on.
 *
 * To run this example locally, you may run Feeder Actor as
 *    `$ bin/run-example org.apache.spark.examples.streaming.akka.FeederActor localhost 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.akka.ActorWordCount localhost 9999`
 */
object ActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: ActorWordCount <hostname> <port>")
      System.exit(1)
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val Seq(host, port) = args.toSeq
    val sparkConf = new SparkConf().setAppName("ActorWordCount")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    // Create the context and set the batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    /*
     * Following is the use of AkkaUtils.createStream to plug in custom actor as receiver
     *
     * An important point to note:
     * Since Actor may exist outside the spark framework, It is thus user's responsibility
     * to ensure the type safety, i.e type of data received and InputDStream
     * should be same.
     *
     * For example: Both AkkaUtils.createStream and SampleActorReceiver are parameterized
     * to same type to ensure type safety.
     */
    val lines = AkkaUtils.createStream[String](
      ssc,
      Props(classOf[SampleActorReceiver[String]],
        s"akka.tcp://test@$host:${port.toInt}/user/FeederActor"),
      "SampleReceiver")

    // compute wordcount
    lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
