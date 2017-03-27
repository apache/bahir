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
package org.apache.bahir.sql.streaming.akka

import java.io.File

import scala.collection.mutable
import scala.util.Random

import akka.actor.{Actor, ActorRef, ActorSystem, ExtendedActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import org.apache.bahir.utils.Logging

class AkkaTestUtils extends Logging {
  private val actorSystemName = "feeder-actor-system"
  private var actorSystem: ActorSystem = _

  private val feederActorName = "feederActor"

  private var message: String = _
  private var count = 1

  def getFeederActorConfig(): Config = {
    val configFile = getClass.getClassLoader
                      .getResource("feeder_actor.conf").getFile
    ConfigFactory.parseFile(new File(configFile))
  }

  def getFeederActorUri(): String =
    s"${actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress}" +
      s"/user/$feederActorName"

  class FeederActor extends Actor {

    val rand = new Random()
    val receivers = new mutable.LinkedHashSet[ActorRef]()

    val sendMessageThread =
      new Thread() {
        override def run(): Unit = {
          var counter = 0
          while (counter < count) {
//            Thread.sleep(500)
            receivers.foreach(_ ! message)
            counter += 1
          }
        }
      }

    override def receive: Receive = {
      case SubscribeReceiver(receiverActor: ActorRef) =>
        log.debug(s"received subscribe from ${receiverActor.toString}")
        receivers += receiverActor
        sendMessageThread.run()

      case UnsubscribeReceiver(receiverActor: ActorRef) =>
        log.debug(s"received unsubscribe from ${receiverActor.toString}")
        receivers -= receiverActor
    }
  }

  def setup(): Unit = {
    val feederConf = getFeederActorConfig()

    actorSystem = ActorSystem(actorSystemName, feederConf)
    actorSystem.actorOf(Props(new FeederActor), feederActorName)
  }

  def shutdown(): Unit = {
//    actorSystem.awaitTermination()
    actorSystem.shutdown()
  }

  def setMessage(message: String): Unit = this.message = message
  def setCountOfMessages(messageCount: Int): Unit = count = messageCount
}
