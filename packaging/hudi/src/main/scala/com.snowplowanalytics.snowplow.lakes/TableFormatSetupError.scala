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

import org.apache.hudi.hive.HoodieHiveSyncException

object TableFormatSetupError {

  // Check if given exception is specific to hudi format
  def check(targetType: String): PartialFunction[Throwable, String] =
    targetType match {
      case "hudi" => {
        case e: HoodieHiveSyncException if e.getMessage.contains("database does not exist") =>
          // Glue database does not exist or no permission to see it
          e.getMessage
      }
      case _ => PartialFunction.empty
    }
}
