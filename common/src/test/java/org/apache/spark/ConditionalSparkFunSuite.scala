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

package org.apache.spark

trait ConditionalSparkFunSuite extends SparkFunSuite {
  /**
   * Run test if given predicate is satisfied.
   * @param testName Test name
   * @param condition If satisfied, test will be executed
   * @param testBody Test body
   */
  def testIf(testName: String, condition: () => Boolean)(testBody: => Unit) {
    if (condition()) {
      test(testName)(testBody)
    } else {
      ignore(testName)(testBody)
    }
  }

  /**
   * Run given code only if predicate has been satisfied.
   * @param condition If satisfied, run code block
   * @param body Code block
   */
  def runIf(condition: () => Boolean)(body: => Unit): Unit = {
    if (condition()) {
      body
    }
  }
}
