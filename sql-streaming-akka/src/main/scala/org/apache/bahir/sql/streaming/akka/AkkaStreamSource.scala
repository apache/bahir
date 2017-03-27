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

package org.apache.bahir.sql.streaming.akka

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Objects}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.rocksdb.{Options, RocksDB}

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import org.apache.bahir.utils.Logging

object AkkaStreamConstants {

  val SCHEMA_DEFAULT = StructType(StructField("value", StringType)
    :: StructField("timestamp", TimestampType) :: Nil)

  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val defaultSupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange =
    15 millis) {
    case _: RuntimeException => Restart
    case _: Exception => Escalate
  }

  val defaultActorSystemCreator: () => ActorSystem = () => {
//    val uniqueSystemName = s"streaming-actor-system-${TaskContext.get().taskAttemptId()}"
    val uniqueSystemName = s"streaming-actor-system"
    val akkaConf = ConfigFactory.parseString(
      s"""akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
         |akka.remote.netty.tcp.port = "0"
         |akka.loggers.0 = "akka.event.slf4j.Slf4jLogger"
         |akka.log-dead-letters-during-shutdown = "off"
       """.stripMargin)
    ActorSystem(uniqueSystemName, akkaConf)
  }
}

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

case class Statistics(numberOfMsgs: Int,
                      numberOfWorkers: Int,
                      numberOfHiccups: Int,
                      otherInfo: String)

private[akka] sealed trait ActorReceiverData
private[akka] case class SingleItemData(item: String) extends ActorReceiverData
private[akka] case class AskStoreSingleItemData(item: String) extends ActorReceiverData
private[akka] case class IteratorData(iterator: Iterator[String]) extends ActorReceiverData
private[akka] case class ByteBufferData(bytes: ByteBuffer) extends ActorReceiverData
private[akka] object Ack extends ActorReceiverData

class AkkaStreamSource(urlOfPublisher: String,
                       persistence: RocksDB, sqlContext: SQLContext,
                       messageParser: String => (String, Timestamp))
  extends Source with Logging {

  override def schema: StructType = AkkaStreamConstants.SCHEMA_DEFAULT

  private val store = new LocalMessageStore(persistence, sqlContext.sparkContext.getConf)

  private val messages = new TrieMap[Int, (String, Timestamp)]()

  private val initLock = new CountDownLatch(1)

  private var offset = 0

  private var actorSystem: ActorSystem = _
  private var actorSupervisor: ActorRef = _

  private def fetchLastProcessedOffset(): Int = {
    Try(store.maxProcessedOffset) match {
      case Success(x) =>
        log.info(s"Recovering from last stored offset $x")
        x
      case Failure(e) => 0
    }
  }

  initialize()
  private def initialize(): Unit = {

    class ActorReceiver(urlOfPublisher: String) extends Actor {

      lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

      override def preStart(): Unit = remotePublisher ! SubscribeReceiver(context.self)

      override def receive: PartialFunction[Any, Unit] = {
        case msg: String => store(msg)
      }

      override def postStop(): Unit = remotePublisher ! UnsubscribeReceiver(context.self)

      def store(iter: Iterator[String]) = {
        context.parent ! IteratorData(iter)
      }

      def store(item: String) = {
        context.parent ! SingleItemData(item)
      }

      def store(item: String, timeout: Timeout): Future[Unit] = {
        context.parent.ask(AskStoreSingleItemData(item))(timeout).map(_ => ())(context.dispatcher)
      }
    }

    class Supervisor extends Actor {
      override val supervisorStrategy = AkkaStreamConstants.defaultSupervisorStrategy

      private val props = Props(new ActorReceiver(urlOfPublisher))
      private val name = "ActorReceiver"
      private val worker = context.actorOf(props, name)
      log.info("Started receiver actor at:" + worker.path)

      private val n: AtomicInteger = new AtomicInteger(0)
      private val hiccups: AtomicInteger = new AtomicInteger(0)

      override def receive: PartialFunction[Any, Unit] = {

        case data =>
          initLock.await()
          var temp = offset + 1

          data match {
            case IteratorData(iterator) =>
              log.debug("received iterator")
              iterator.asInstanceOf[Iterator[String]].foreach(record => {
                messages.put(temp, messageParser(record.toString))
                temp += 1
              })

            case SingleItemData(msg) =>
              log.debug("received single")
              messages.put(temp, messageParser(msg))
              n.incrementAndGet()

            case AskStoreSingleItemData(msg) =>
              log.debug("received single sync")
              messages.put(temp, messageParser(msg))
              n.incrementAndGet()
              sender() ! Ack

            case ByteBufferData(bytes) =>
              log.debug("received bytes")
              messages.put(temp, messageParser(new String(bytes.array())))

            case props: Props =>
              val worker = context.actorOf(props)
              log.info("Started receiver worker at:" + worker.path)
              sender() ! worker

            case (props: Props, name: String) =>
              val worker = context.actorOf(props, name)
              log.info("Started receiver worker at:" + worker.path)
              sender() ! worker

            case _: PossiblyHarmful => hiccups.incrementAndGet()

            case _: Statistics =>
              val workers = context.children
              sender() ! Statistics(n.get(), workers.size, hiccups.get(), workers.mkString("\n"))
          }
          offset = temp
      }
    }

    actorSystem = AkkaStreamConstants.defaultActorSystemCreator()
    actorSupervisor = actorSystem.actorOf(Props(new Supervisor), "Supervisor")
    offset = fetchLastProcessedOffset()
    initLock.countDown()
  }

  override def stop(): Unit = {
    actorSupervisor ! PoisonPill
    Persistence.close()
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  override def getOffset: Option[Offset] = {
    if (offset == 0) {
      None
    } else {
      Some(LongOffset(offset))
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startIndex = start.getOrElse(LongOffset(0L)).asInstanceOf[LongOffset].offset.toInt
    val endIndex = end.asInstanceOf[LongOffset].offset.toInt
    val data: ArrayBuffer[(String, Timestamp)] = ArrayBuffer.empty

    ((startIndex + 1) to endIndex).foreach { id =>
      val element: (String, Timestamp) = messages.getOrElse(id,
        store.retrieve[(String, Timestamp)](id).orNull)

      if (!Objects.isNull(element)) {
        data += element
        store.store(id, element)
      }
      messages.remove(id, element)
    }
    log.trace(s"Get Batch invoked, ${data.mkString}")
    import sqlContext.implicits._
    data.toDF("value", "timestamp")
  }
}

class AkkaStreamSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
                            providerName: String, parameters: Map[String, String])
  : (String, StructType) = ("akka", AkkaStreamConstants.SCHEMA_DEFAULT)

  override def createSource(sqlContext: SQLContext, metadataPath: String,
                            schema: Option[StructType], providerName: String,
                            parameters: Map[String, String]): Source = {

    def e(s: String) = new IllegalArgumentException(s)

    val urlOfPublisher: String = parameters.getOrElse("urlOfPublisher", parameters.getOrElse("path",
      throw e(
        s"""Please provide url of Publisher actor by specifying path
           | or .options("urlOfPublisher",...)""".stripMargin)))

    val persistenceDirPath: String = parameters.getOrElse("persistenceDirPath",
      System.getProperty("java.io.tmpdir"))

    val messageParserWithTimestamp = (x: String) =>
      (x, Timestamp.valueOf(AkkaStreamConstants.DATE_FORMAT.format(Calendar.getInstance().getTime)))

    val persistence = Persistence.getOrCreatePersistenceInstance(persistenceDirPath)
    new AkkaStreamSource(urlOfPublisher, persistence, sqlContext, messageParserWithTimestamp)
  }

  override def shortName(): String = "akka"
}

object Persistence {
  var persistence: RocksDB = _

  def getOrCreatePersistenceInstance(persistenceDirPath: String): RocksDB = {
    if (Objects.isNull(persistence)) {
      RocksDB.loadLibrary()
      persistence = RocksDB.open(new Options().setCreateIfMissing(true), persistenceDirPath)
    }
    persistence
  }

  def close(): Unit = {
    if (!Objects.isNull(persistence)) {
      persistence.close()
      persistence = null
    }
  }
}
