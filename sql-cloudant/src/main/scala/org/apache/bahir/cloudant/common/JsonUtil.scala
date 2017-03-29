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
package org.apache.bahir.cloudant.common

import play.api.libs.json.{JsUndefined, JsValue}
import scala.util.control.Breaks._

object JsonUtil {
  def getField(row: JsValue, field: String) : Option[JsValue] = {
    var path = field.split('.')
    var currentValue = row
    var finalValue: Option[JsValue] = None
    breakable {
      for (i <- path.indices) {
        val f: Option[JsValue] = (currentValue \ path(i)).toOption
        f match {
          case Some(f2) => currentValue = f2
          case None => break
        }
        if (i == path.length -1) {
          // The leaf node
          finalValue = Some(currentValue)
        }
      }
    }
    finalValue
  }
}
