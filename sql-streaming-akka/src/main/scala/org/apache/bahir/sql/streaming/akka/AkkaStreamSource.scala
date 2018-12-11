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
import java.util
import java.util.{Calendar, Objects, Optional}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.rocksdb.{Options, RocksDB}

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UTF8StringBuilder
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, MicroBatchReadSupport}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
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

class AkkaMicroBatchReader(urlOfPublisher: String,
                           persistence: RocksDB,
                           messageParser: String => (String, Timestamp))
  extends MicroBatchReader with Logging {

  private val store = new LocalMessageStore(persistence, SparkEnv.get.conf)

  private val messages = new TrieMap[Long, (String, Timestamp)]()

  private val initLock = new CountDownLatch(1)

  @GuardedBy("this")
  private var currentOffset: LongOffset = LongOffset(-1L)

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private var startOffset: Offset = _
  private var endOffset: Offset = _

  private var actorSystem: ActorSystem = _
  private var actorSupervisor: ActorRef = _


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
          var temp = currentOffset.offset + 1

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
          currentOffset = LongOffset(temp)
      }
    }

    actorSystem = AkkaStreamConstants.defaultActorSystemCreator()
    actorSupervisor = actorSystem.actorOf(Props(new Supervisor), "Supervisor")
    if (store.maxProcessedOffset > 0) {
      currentOffset = LongOffset(store.maxProcessedOffset)
    }
    initLock.countDown()
  }

  // This method is only used for unit test
  private[akka] def getCurrentOffset: LongOffset = {
    currentOffset.copy()
  }


  override def getEndOffset: Offset = {
    Option(endOffset).getOrElse(throw new IllegalStateException("end offset not set"))
  }

  override def getStartOffset: Offset = {
    Option(startOffset).getOrElse(throw new IllegalStateException("start offset not set"))
  }

  override def setOffsetRange(start: Optional[Offset],
                              end: Optional[Offset]): Unit = synchronized {
    startOffset = start.orElse(LongOffset(-1L))
    endOffset = end.orElse(currentOffset)
  }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = LongOffset.convert(end).getOrElse(
      sys.error(s"AkkaMicroBatchReader.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = newOffset.offset - lastOffsetCommitted.offset

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    (lastOffsetCommitted.offset until newOffset.offset).foreach { x =>
      messages.remove(x + 1)
    }
    lastOffsetCommitted = newOffset
  }

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def stop(): Unit = {
    actorSupervisor ! PoisonPill
    Persistence.close()
    Await.ready(actorSystem.terminate(), Duration.Inf)
  }

  override def readSchema(): StructType = AkkaStreamConstants.SCHEMA_DEFAULT

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    assert(startOffset != null && endOffset != null,
      "start offset and end offset should already be set before create read tasks.")

    val (start, end) = synchronized {
      (LongOffset.convert(startOffset).get.offset + 1, LongOffset.convert(endOffset).get.offset + 1)
    }
    val rawList = for (i <- start until end) yield {
      store.store(i, messages(i))
      messages(i)
    }

    val numPartitions = SparkSession.getActiveSession.get.sparkContext.defaultParallelism

    val slices = Array.fill(numPartitions)(new ArrayBuffer[(String, Timestamp)])
    rawList.zipWithIndex.foreach { case (r, idx) =>
      slices(idx % numPartitions).append(r)
    }

    (0 until numPartitions).map { i =>
      val slice = slices(i)
      new InputPartition[InternalRow] {
        override def createPartitionReader(): InputPartitionReader[InternalRow] =
            new InputPartitionReader[InternalRow] {
          private var currentIdx = -1

          override def next(): Boolean = {
            currentIdx += 1
            currentIdx < slice.size
          }

          override def get(): InternalRow = {
            val builder = new UTF8StringBuilder()
            builder.append(slice(currentIdx)._1)
            InternalRow(builder.build(), slice(currentIdx)._2.getTime)
          }

          override def close(): Unit = {}
        }
      }
    }.toList.asJava
  }

}

class AkkaStreamSourceProvider extends MicroBatchReadSupport with DataSourceRegister with Logging {

  override def shortName(): String = "akka"

  override def createMicroBatchReader(schema: Optional[StructType],
                                      metadataPath: String,
                                      options: DataSourceOptions): MicroBatchReader = {
    val parameters = options.asMap().asScala.toMap

    val urlOfPublisher: String = parameters.getOrElse("urlofpublisher", parameters.getOrElse("path",
      throw new IllegalArgumentException(
        s"""Please provide url of Publisher actor by specifying path
           | or .options("urlOfPublisher",...)""".stripMargin)))

    val persistenceDirPath: String = parameters.getOrElse("persistencedirpath",
      System.getProperty("java.io.tmpdir"))

    val messageParserWithTimestamp = (x: String) =>
      (x, Timestamp.valueOf(AkkaStreamConstants.DATE_FORMAT.format(Calendar.getInstance().getTime)))

    val persistence = Persistence.getOrCreatePersistenceInstance(persistenceDirPath)
    new AkkaMicroBatchReader(urlOfPublisher, persistence, messageParserWithTimestamp)
  }
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
