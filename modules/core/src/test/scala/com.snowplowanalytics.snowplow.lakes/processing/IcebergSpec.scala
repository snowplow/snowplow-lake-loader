/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.processing

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.snowplowanalytics.snowplow.lakes.TestConfig

import fs2.io.file.Path

class IcebergSpec extends AbstractSparkSpec {

  override def target: TestConfig.Target = TestConfig.Iceberg

  override def supportsRequiredNestedFields: Boolean = true

  /** Reads the table back into memory, so we can make assertions on the app's output */
  override def readTable(spark: SparkSession, tmpDir: Path): DataFrame =
    spark.sql("select * from test_catalog.test.events")

  /** Spark config used only while reading table back into memory for assertions */
  override def sparkConfig(tmpDir: Path): Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.test_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.test_catalog.type" -> "hadoop",
      "spark.sql.catalog.test_catalog.warehouse" -> tmpDir.toString
    )
}
