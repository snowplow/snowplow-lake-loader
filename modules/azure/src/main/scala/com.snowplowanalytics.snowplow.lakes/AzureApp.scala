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

import java.net.UnknownHostException

import cats.implicits._
import cats.effect.IO

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.{AbfsRestOperationException, InvalidConfigurationValueException}

import com.snowplowanalytics.snowplow.streams.kafka.{KafkaFactory, KafkaSinkConfig, KafkaSourceConfig}

object AzureApp extends LoaderApp[Unit, KafkaSourceConfig, KafkaSinkConfig](BuildInfo) {

  override def toFactory: FactoryProvider =
    _ => KafkaFactory.resource[IO]

  override def isDestinationSetupError(targetType: String): DestinationSetupErrorCheck =
    isAzureSetupError.orElse(TableFormatSetupError.check(targetType))

  private def isAzureSetupError: DestinationSetupErrorCheck = {
    // Authentication issue (wrong OAuth endpoint, wrong client id, wrong secret)
    case AuthenticationError(e) =>
      e
    // Wrong container name
    case e: AbfsRestOperationException if e.getStatusCode === 404 =>
      s"The specified filesystem does not exist (e.g. wrong container name)"
    // Service principal missing permissions for container (role assignement missing or wrong role)
    case e: AbfsRestOperationException if e.getStatusCode === 403 =>
      s"Missing permissions for the destination (needs \"Storage Blob Data Contributor\" assigned to the service principal for the container)"
    // Soft delete not disabled
    case e: AbfsRestOperationException if e.getStatusCode === 409 =>
      "Blob soft delete must be disabled on the storage account"
    // Invalid storage container path
    case _: InvalidConfigurationValueException =>
      "Invalid storage container path"
    case _: UnknownHostException =>
      "Wrong storage name"
  }
}
