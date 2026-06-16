/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

// Intentionally placed in org.apache.spark.sql so that scalac permits the call to
// SparkSession.internalCreateDataFrame, which is private[sql].
//
// reattachSchema passes the DataFrame's existing RDD[InternalRow] directly to
// internalCreateDataFrame, bypassing the InternalRow→Row→InternalRow roundtrip that
// the public createDataFrame(rdd: RDD[Row], schema) incurs. The isStreaming parameter
// defaults to false, which is correct for the batch accumulation use case here.
// Not part of the public API of this project.
object SnowplowInternalSparkBridge {
  def reattachSchema(df: DataFrame, schema: StructType): DataFrame =
    df.sparkSession.internalCreateDataFrame(df.queryExecution.toRdd, schema)
}
