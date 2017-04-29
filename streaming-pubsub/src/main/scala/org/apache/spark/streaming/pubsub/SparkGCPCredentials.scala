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

package org.apache.spark.streaming.pubsub

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.services.pubsub.PubsubScopes
import com.google.cloud.hadoop.util.{EntriesCredentialConfiguration, HadoopCredentialConfiguration}
import java.util
import org.apache.hadoop.conf.Configuration

/**
 * Serializable interface providing a method executors can call to obtain an
 * GCPCredentialsProvider instance for authenticating to GCP services.
 */
private[pubsub] sealed trait SparkGCPCredentials extends Serializable {

  val PUBSUB_PREFIX = "sparkstreaming.pubsub"

  def provider: Credential
}

/**
 * Returns application default type credential
 */
private[pubsub] final case object ApplicationDefaultCredentials extends SparkGCPCredentials {

  override def provider: Credential = {
    GoogleCredential.getApplicationDefault.createScoped(PubsubScopes.all())
  }
}

/**
 * Returns a Service Account type Credential instance.
 *
 * @param jsonFilePath file path for json
 * @param p12FilePath  file path for p12
 * @param emailAccount email account for p12
 */
private[pubsub] final case class ServiceAccountCredentials(
    jsonFilePath: Option[String] = None,
    p12FilePath: Option[String] = None,
    emailAccount: Option[String] = None)
    extends SparkGCPCredentials {

  override def provider: Credential = {
    val conf = new Configuration(false)
    conf.setBoolean(
      PUBSUB_PREFIX + EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
      true)
    jsonFilePath match {
      case Some(jsonFilePath) =>
        conf.set(
          PUBSUB_PREFIX + EntriesCredentialConfiguration.JSON_KEYFILE_SUFFIX,
          jsonFilePath
        )
      case None =>
        p12FilePath match {
          case Some(p12FilePath) =>
            conf.set(
              PUBSUB_PREFIX + EntriesCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX,
              p12FilePath
            )
        }
        emailAccount match {
          case Some(emailAccount) =>
            conf.set(
              PUBSUB_PREFIX + EntriesCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX,
              emailAccount
            )
        }
    }

    HadoopCredentialConfiguration
        .newBuilder()
        .withConfiguration(conf)
        .withOverridePrefix(PUBSUB_PREFIX)
        .build()
        .getCredential(new util.ArrayList(PubsubScopes.all()))
  }

}
