/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import cats.implicits._
import cats.effect.IO

import com.google.api.client.auth.oauth2.TokenResponseException
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.storage.StorageException

import com.snowplowanalytics.snowplow.streams.pubsub.{PubsubFactory, PubsubFactoryConfig, PubsubSinkConfig, PubsubSourceConfig}

object GcpApp extends LoaderApp[PubsubFactoryConfig, PubsubSourceConfig, PubsubSinkConfig](BuildInfo) {

  override def toFactory: FactoryProvider = config => PubsubFactory.resource[IO](config)

  override def isDestinationSetupError: DestinationSetupErrorCheck = {
    // Bad Request - Key belongs to nonexistent service account
    case e: TokenResponseException if e.getStatusCode === 400 =>
      "The service account key is invalid"
    // Forbidden - Permissions missing for Cloud Storage
    case e: GoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(403) =>
      e.getDetails.getMessage
    // Not Found - Destination bucket doesn't exist
    case e: GoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(404) =>
      "The specified bucket does not exist"
    // Forbidden - Permissions missing for Cloud Storage
    case e: StorageException if Option(e.getCode).contains(403) =>
      "IAM role is missing permissions"
  }
}
