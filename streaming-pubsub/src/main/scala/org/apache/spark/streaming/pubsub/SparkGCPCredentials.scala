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

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.HttpTransport
import com.google.api.client.json.jackson.JacksonFactory
import com.google.api.services.pubsub.PubsubScopes
import com.google.cloud.hadoop.util.{CredentialFactory, HttpTransportFactory}

/**
 * Serializable interface providing a method executors can call to obtain an
 * GCPCredentialsProvider instance for authenticating to GCP services.
 */
private[pubsub] sealed trait SparkGCPCredentials extends Serializable {
  def provider: Credential

  def jacksonFactory(): JacksonFactory = new JacksonFactory

  def httpTransport(): HttpTransport = HttpTransportFactory.createHttpTransport(
    HttpTransportFactory.HttpTransportType.JAVA_NET, null
  )

  def scopes(): util.Collection[String] = PubsubScopes.all()
}

/**
 * Application default credentials.
 */
private[pubsub] final case object ApplicationDefaultCredentials extends SparkGCPCredentials {
  override def provider: Credential = {
    GoogleCredential.getApplicationDefault.createScoped(PubsubScopes.all())
  }
}

/**
 * Credentials based on JSON key file.
 */
private[pubsub] final case class JsonConfigCredentials(jsonContent: Array[Byte])
    extends SparkGCPCredentials {
  def this(jsonFilePath: String) = this(Files.readAllBytes(Paths.get(jsonFilePath)))

  override def provider: Credential = {
    val stream = new ByteArrayInputStream(jsonContent)
    val credentials = CredentialFactory.GoogleCredentialWithRetry.fromGoogleCredential(
      GoogleCredential.fromStream(
        stream, httpTransport(), jacksonFactory()
      ).createScoped(scopes())
    )
    stream.close()

    credentials
  }
}

/**
 * Credentials based on e-mail account and P12 key.
 */
private[pubsub] final case class EMailPrivateKeyCredentials(
    emailAccount: String, p12Content: Array[Byte]
  ) extends SparkGCPCredentials {
  def this(emailAccount: String, p12FilePath: String) = {
    this(emailAccount, Files.readAllBytes(Paths.get(p12FilePath)))
  }

  override def provider: Credential = {
    val tempFile = File.createTempFile(emailAccount, ".p12")
    tempFile.deleteOnExit()
    val p12Out = Files.newOutputStream(tempFile.toPath)
    p12Out.write(p12Content, 0, p12Content.length)
    p12Out.flush()
    p12Out.close()

    new CredentialFactory.GoogleCredentialWithRetry(
      new GoogleCredential.Builder().setTransport(httpTransport())
        .setJsonFactory(jacksonFactory())
        .setServiceAccountId(emailAccount)
        .setServiceAccountScopes(scopes())
        .setServiceAccountPrivateKeyFromP12File(tempFile)
        .setRequestInitializer(new CredentialFactory.CredentialHttpRetryInitializer())
    )
  }
}

/**
 * Credentials based on metadata service.
 */
private[pubsub] final case class MetadataServiceCredentials() extends SparkGCPCredentials {
  override def provider: Credential = {
    (new CredentialFactory).getCredentialFromMetadataServiceAccount
  }
}

object SparkGCPCredentials {
  /**
   * Builder for SparkGCPCredentials instance.
   */
  class Builder {
    private var credentials: Option[SparkGCPCredentials] = None

    /**
     * Use a JSON type key file for service account credential
     * @param jsonFilePath JSON type key file
     * @return Reference to this SparkGCPCredentials.Builder
     */
    def jsonServiceAccount(jsonFilePath: String): Builder = {
      credentials = Option(new JsonConfigCredentials(jsonFilePath))
      this
    }

    /**
     * Use a JSON type key file for service account credential
     * @param jsonFileBuffer binary content of JSON type key file
     * @return Reference to this SparkGCPCredentials.Builder
     */
    def jsonServiceAccount(jsonFileBuffer: Array[Byte]): Builder = {
      credentials = Option(JsonConfigCredentials(jsonFileBuffer))
      this
    }

    /**
     * Use a P12 type key file service account credential
     * @param p12FilePath P12 type key file
     * @param emailAccount email of service account
     * @return Reference to this SparkGCPCredentials.Builder
     */
    def p12ServiceAccount(p12FilePath: String, emailAccount: String): Builder = {
      credentials = Option(new EMailPrivateKeyCredentials(emailAccount, p12FilePath))
      this
    }

    /**
     * Use a P12 type key file service account credential
     * @param p12FileBuffer binary content of P12 type key file
     * @param emailAccount email of service account
     * @return Reference to this SparkGCPCredentials.Builder
     */
    def p12ServiceAccount(p12FileBuffer: Array[Byte], emailAccount: String): Builder = {
      credentials = Option(EMailPrivateKeyCredentials(emailAccount, p12FileBuffer))
      this
    }

    /**
     * Use a meta data service to return service account
     * @return Reference to this SparkGCPCredentials.Builder
     */
    def metadataServiceAccount(): Builder = {
      credentials = Option(MetadataServiceCredentials())
      this
    }

    /**
     * Returns the appropriate instance of SparkGCPCredentials given the configured
     * parameters.
     * - The service account credentials will be returned if they were provided.
     * - The application default credentials will be returned otherwise.
     * @return SparkGCPCredentials object
     */
    def build(): SparkGCPCredentials = credentials.getOrElse(ApplicationDefaultCredentials)
  }

  /**
   * Creates a SparkGCPCredentials.Builder for constructing
   * SparkGCPCredentials instance.
   * @return SparkGCPCredentials.Builder instance
   */
  def builder: Builder = new Builder
}
