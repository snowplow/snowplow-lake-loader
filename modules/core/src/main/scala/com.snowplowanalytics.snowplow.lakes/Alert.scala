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

import cats.Show
import cats.implicits.showInterpolator

import com.snowplowanalytics.snowplow.runtime.SetupExceptionMessages

sealed trait Alert
object Alert {

  final case class FailedToCreateEventsTable(causes: SetupExceptionMessages) extends Alert
  final case class FailedToCommitToLake(causes: SetupExceptionMessages) extends Alert

  implicit def showAlert: Show[Alert] = Show[Alert] {
    case FailedToCreateEventsTable(causes) =>
      show"Failed to create events table: $causes"
    case FailedToCommitToLake(causes) =>
      show"Failed to commit to lake: $causes"
  }
}
