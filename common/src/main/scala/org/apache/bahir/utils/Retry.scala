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

package org.apache.bahir.utils

object Retry {
  /**
   * Retry invocation of given code.
   * @param attempts Number of attempts to try executing given code. -1 represents infinity.
   * @param pauseMs Number of backoff milliseconds.
   * @param retryExceptions Types of exceptions to retry.
   * @param code Function to execute.
   * @tparam A Type parameter.
   * @return Returns result of function execution or exception in case of failure.
   */
  def apply[A](attempts: Int, pauseMs: Long, retryExceptions: Class[_]*)(code: => A): A = {
    var result: Option[A] = None
    var success = false
    var remaining = attempts
    while (!success) {
      try {
        remaining -= 1
        result = Some(code)
        success = true
      }
      catch {
        case e: Exception =>
          if (retryExceptions.contains(e.getClass) && (attempts == -1 || remaining > 0)) {
            Thread.sleep(pauseMs)
          } else {
            throw e
          }
      }
    }
    result.get
  }
}
