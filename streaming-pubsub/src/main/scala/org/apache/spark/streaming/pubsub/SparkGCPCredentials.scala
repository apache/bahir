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
import com.google.api.client.json.jackson.JacksonFactory
import com.google.api.services.pubsub.PubsubScopes
import com.google.cloud.hadoop.util.{CredentialFactory, HttpTransportFactory}
import java.io.{ByteArrayInputStream, File, FileNotFoundException, FileOutputStream}
import java.nio.file.{Files, Paths}
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
  private val fileBytes = getFileBuffer

  override def provider: Credential = {
    val jsonFactory = new JacksonFactory
    val scopes = new util.ArrayList(PubsubScopes.all())
    val transport = HttpTransportFactory.createHttpTransport(
      HttpTransportFactory.HttpTransportType.JAVA_NET, null)

    if (!jsonFilePath.isEmpty) {
      val stream = new ByteArrayInputStream(fileBytes)
      CredentialFactory.GoogleCredentialWithRetry.fromGoogleCredential(
        GoogleCredential.fromStream(stream, transport, jsonFactory)
        .createScoped(scopes))
    } else if (!p12FilePath.isEmpty && !emailAccount.isEmpty) {
      val tempFile = File.createTempFile(emailAccount.get, ".p12")
      tempFile.deleteOnExit
      val p12Out = new FileOutputStream(tempFile)
      p12Out.write(fileBytes, 0, fileBytes.length)
      p12Out.close

      new CredentialFactory.GoogleCredentialWithRetry(
        new GoogleCredential.Builder().setTransport(transport)
        .setJsonFactory(jsonFactory)
        .setServiceAccountId(emailAccount.get)
        .setServiceAccountScopes(scopes)
        .setServiceAccountPrivateKeyFromP12File(tempFile)
        .setRequestInitializer(new CredentialFactory.CredentialHttpRetryInitializer()))
    } else (new CredentialFactory).getCredentialFromMetadataServiceAccount
  }

  private def getFileBuffer: Array[Byte] = {
    val filePath = jsonFilePath orElse p12FilePath
    if (filePath.isEmpty) Array[Byte]()
    else if (!Files.exists(Paths.get(filePath.get))) {
      throw new FileNotFoundException(s"The key file path(${filePath.get}) doesn't exist.")
    } else Files.readAllBytes(Paths.get(filePath.get))
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
