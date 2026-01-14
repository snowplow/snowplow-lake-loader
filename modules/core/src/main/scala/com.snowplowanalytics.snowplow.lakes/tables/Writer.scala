/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.tables

import cats.effect.Sync
import org.apache.spark.sql.{DataFrame, SparkSession}

/** The methods needed for a writing specific table format (e.g. delta or iceberg) */
trait Writer {

  /** Spark config parameters which the Lake Loader needs for this specific table format */
  def sparkConfig: Map[String, String]

  /**
   * Prepare a table to be ready for loading. Runs once when the app first starts up.
   *
   * For some table formats that can mean creating the table, or registering it in an external
   * catalog.
   */
  def prepareTable[F[_]: Sync](spark: SparkSession): F[Unit]

  /** Write Snowplow events into the table */
  def write[F[_]: Sync](df: DataFrame): F[Unit]

  /** Get the total number of active data files in the table */
  def getTableDataFilesTotal[F[_]: Sync](spark: SparkSession): F[Option[Long]]

  /** Get the total number of table snapshots/versions currently retained in the transaction log */
  def getTableSnapshotsRetained[F[_]: Sync](spark: SparkSession): F[Option[Long]]

  /**
   * Whether this writer expects the DataFrame to be sorted by the partition column, i.e. by
   * event_name
   */
  def expectsSortedDataframe: Boolean
}
