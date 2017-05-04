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
package org.apache.spark.examples.streaming.twitter

import org.apache.log4j.{Level, Logger}
import twitter4j.FilterQuery

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

/**
 * Illustrates the use of custom filter queries to get Tweets from one or more locations.
 */
object TwitterLocations {
  def main(args: Array[String]) {
    if (args.length < 4 || args.length % 4 != 0) {
      System.err.println("Usage: TwitterLocations <consumer key> <consumer secret> " +
        "<access token> <access token secret> " +
        "[<latitude-south-west> <longitude-south-west>" +
        " <latitude-north-east> <longitude-north-east> ...]")
      System.exit(1)
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Get bounding boxes of locations for which to retrieve Tweets from command line
    val locationArgs = args.takeRight(args.length - 4)
    val boundingBoxes = if (locationArgs.length == 0) {
      System.out.println("No location bounding boxes specified, using defaults for New York City")
      val nycSouthWest = Array(-74.0, 40.0)
      val nycNorthEast = Array(-73.0, 41.0)
      Array(nycSouthWest, nycNorthEast)
    } else {
      locationArgs.map(_.toDouble).sliding(2, 2).toArray
    }

    val sparkConf = new SparkConf().setAppName("TwitterLocations")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val locationsQuery = new FilterQuery().locations(boundingBoxes : _*)

    // Print Tweets from the specified coordinates
    // This includes Tweets geo-tagged in the bounding box defined by the coordinates
    // As well as Tweets tagged in places inside of the bounding box
    TwitterUtils.createFilteredStream(ssc, None, Some(locationsQuery))
      .map(tweet => {
        val latitude = Option(tweet.getGeoLocation).map(l => s"${l.getLatitude},${l.getLongitude}")
        val place = Option(tweet.getPlace).map(_.getName)
        val location = latitude.getOrElse(place.getOrElse("(no location)"))
        val text = tweet.getText.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        s"$location\t$text"
      })
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
