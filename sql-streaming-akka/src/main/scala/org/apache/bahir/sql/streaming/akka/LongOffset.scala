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

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2.reader.streaming.{Offset => OffsetV2}

/**
 * @note As of 2.3.0, [[org.apache.spark.sql.execution.streaming.LongOffset]]
 *       hasn't extended v2 Offset yet. Fix version is 3.0.0. Until then
 *       this is a required class.
 * @see SPARK-23092
 */
case class LongOffset(offset: Long) extends OffsetV2 {

  override val json = offset.toString

  def +(increment: Long): LongOffset = new LongOffset(offset + increment)
  def -(decrement: Long): LongOffset = new LongOffset(offset - decrement)
}

object LongOffset {

  /**
   * LongOffset factory from serialized offset.
   * @return new LongOffset
   */
  def apply(offset: SerializedOffset) : LongOffset = new LongOffset(offset.json.toLong)

  /**
   * Convert generic Offset to LongOffset if possible.
   * @return converted LongOffset
   */
  def convert(offset: Offset): Option[LongOffset] = offset match {
    case lo: LongOffset => Some(lo)
    case so: SerializedOffset => Some(LongOffset(so))
    case _ => None
  }
}
