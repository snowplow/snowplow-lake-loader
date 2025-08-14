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

import org.apache.iceberg.exceptions.{ForbiddenException => IcebergForbiddenException, NoSuchIcebergTableException, NotFoundException => IcebergNotFoundException}

import org.apache.spark.sql.delta.DeltaAnalysisException

object TableFormatSetupError {

  // Check if given exception is specific to iceberg format
  def check: PartialFunction[Throwable, String] = {
      case _: NoSuchIcebergTableException =>
        // Table exists but not in Iceberg format
        "Target table is not an Iceberg table"
      case e: IcebergNotFoundException =>
        // Glue catalog does not exist
        e.getMessage
      case e: IcebergForbiddenException =>
        // No permission to create a table in Glue catalog
        e.getMessage
      case e: DeltaAnalysisException if e.errorClass == Some("DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION") =>
        "Destination not empty and not a Delta table"
    }
}
