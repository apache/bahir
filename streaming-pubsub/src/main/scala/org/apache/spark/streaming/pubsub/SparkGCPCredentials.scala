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
 * If all parameters are None, then try metadata service type
 * If jsonFilePath available, try json type
 * If jsonFilePath is None and p12FilePath and emailAccount available, try p12 type
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
      EntriesCredentialConfiguration.BASE_KEY_PREFIX
          + EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
      true)
    jsonFilePath match {
      case Some(jsonFilePath) =>
        conf.set(
          EntriesCredentialConfiguration.BASE_KEY_PREFIX
              + EntriesCredentialConfiguration.JSON_KEYFILE_SUFFIX,
          jsonFilePath
        )
      case _ => // do nothing
    }
    p12FilePath match {
      case Some(p12FilePath) =>
        conf.set(
          EntriesCredentialConfiguration.BASE_KEY_PREFIX
              + EntriesCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX,
          p12FilePath
        )
      case _ => // do nothing
    }
    emailAccount match {
      case Some(emailAccount) =>
        conf.set(
          EntriesCredentialConfiguration.BASE_KEY_PREFIX
              + EntriesCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX,
          emailAccount
        )
      case _ => // do nothing
    }

    HadoopCredentialConfiguration
        .newBuilder()
        .withConfiguration(conf)
        .build()
        .getCredential(new util.ArrayList(PubsubScopes.all()))
  }

}

object SparkGCPCredentials {

  /**
   * Builder for SparkGCPCredentials instance.
   */
  class Builder {
    private var creds: Option[SparkGCPCredentials] = None

    /**
     * Use a json type key file for service account credential
     *
     * @param jsonFilePath json type key file
     * @return Reference to this SparkGCPCredentials.Builder
     */
    def jsonServiceAccount(jsonFilePath: String): Builder = {
      creds = Option(ServiceAccountCredentials(Option(jsonFilePath)))
      this
    }

    /**
     * Use a p12 type key file service account credential
     *
     * @param p12FilePath p12 type key file
     * @param emailAccount email of service account
     * @return Reference to this SparkGCPCredentials.Builder
     */
    def p12ServiceAccount(p12FilePath: String, emailAccount: String): Builder = {
      creds = Option(ServiceAccountCredentials(
        p12FilePath = Option(p12FilePath), emailAccount = Option(emailAccount)))
      this
    }

    /**
     * Use a meta data service to return service account
     * @return Reference to this SparkGCPCredentials.Builder
     */
    def metadataServiceAccount(): Builder = {
      creds = Option(ServiceAccountCredentials())
      this
    }

    /**
     * Returns the appropriate instance of SparkGCPCredentials given the configured
     * parameters.
     *
     * - The service account credentials will be returned if they were provided.
     *
     * - The application default credentials will be returned otherwise.
     * @return
     */
    def build(): SparkGCPCredentials = creds.getOrElse(ApplicationDefaultCredentials)

  }

  /**
   * Creates a SparkGCPCredentials.Builder for constructing
   * SparkGCPCredentials instance.
   *
   * @return SparkGCPCredentials.Builder instance
   */
  def builder: Builder = new Builder
}
