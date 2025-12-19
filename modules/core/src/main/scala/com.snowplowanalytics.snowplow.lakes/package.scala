/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow

package object lakes {
  type AnyConfig = Config[Any, Any, Any]

  /**
   * Function that checks whether an exception is due to a destination setup error
   *
   * If an exception was caused by a destination setup error, then it should return a short
   * human-friendly description of the problem. For any other exception it should return nothing.
   *
   * A DestinationSetupErrorCheck should check the top-level exception only; it should NOT check
   * `getCause`. Because our application code already checks the causes.
   */
  type DestinationSetupErrorCheck = PartialFunction[Throwable, String]
}
