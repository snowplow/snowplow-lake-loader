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

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException

import cats.implicits._

object AuthenticationError {

  private val knownErrors = Map(
    90002 -> "Tenant is invalid or does not have an active subscription",
    700016 -> "Application ID was not found for this tenant",
    7000215 -> "Invalid client secret provided",
    7000222 -> "Client secret keys have expired"
  )

  def unapply(throwable: Throwable): Option[String] = throwable match {
    case e: AbfsRestOperationException if e.getStatusCode == -1 =>
      extractErrorCode(e.getMessage).flatMap(knownErrors.get).map(err => s"Could not authenticate. $err")
    case _ =>
      None
  }

  private def extractErrorCode(errorMessage: String): Option[Int] = {
    val regex = """.*\bAADSTS(\d+)\b.*""".r
    Either.catchNonFatal {
      val regex(errorCode) = errorMessage
      errorCode.toInt
    }.toOption
  }
}
