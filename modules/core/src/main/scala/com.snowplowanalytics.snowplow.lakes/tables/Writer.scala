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

  /**
   * Write Snowplow events into the table.
   *
   * Both formats sort each writer partition by the table's partition columns and keep one output
   * file open at a time, reached differently: Iceberg from `fanout-enabled` being false in
   * `icebergWriteOptions`, Delta from Spark's `FileFormatWriter`, whose concurrent-writer path
   * needs `spark.sql.maxConcurrentOutputFileWriters`. The alternative in both is a writer that
   * keeps a file open per partition value the task meets, and an open parquet writer holds its
   * column dictionaries and buffers on the JVM heap, so heap residency would scale with the
   * distinct partition values in a task rather than with anything the loader sizes. Those values
   * reduce to the task's distinct `event_name`s: Spark's `ComputeCurrentTime` folds the
   * `current_timestamp()` that `SparkUtils.readFinalDataFrame` assigns to `load_tstamp` into one
   * literal for the whole write, so the date half of the partition spec is constant.
   *
   * The sort is a Tungsten sort, so it draws execution memory from the area a window is staged in
   * and spills to `spark.local.dir` rather than failing when the area is short. What it asks for is
   * a window's rows, not one partition's: in local mode every one of `LakeWriter`'s writer tasks
   * sorts at once. That is what empties the area every window, and why `prepareCommit` releases a
   * window's staged batches before reaching here rather than after.
   *
   * For Iceberg the sort is load-bearing rather than an optimisation. `prepareFinalDataFrame`
   * clusters rows into a partition but leaves them in shuffle order, and the clustered writer
   * throws when a row arrives for a partition whose file it has already closed - so a Spark or
   * Iceberg upgrade that stopped applying the required ordering would fail commits outright.
   * `IcebergWriterSpec` pins that it is still applied.
   */
  def write[F[_]: Sync](df: DataFrame): F[Unit]

  /**
   * A summary of the table as it actually exists, one element per line.
   *
   * Reads the table rather than the config, because both formats create a table only if it is
   * absent: an existing table keeps its own properties. Iceberg drops the `TBLPROPERTIES` of a
   * `CREATE TABLE IF NOT EXISTS` silently, and Delta raises
   * `DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY`, which `DeltaWriter.prepareTable` catches.
   */
  def describeTable[F[_]: Sync](spark: SparkSession): F[List[String]]

  /** Get the total number of active data files in the table */
  def getTableDataFilesTotal[F[_]: Sync](spark: SparkSession): F[Option[Long]]

  /** Get the total number of table snapshots/versions currently retained in the transaction log */
  def getTableSnapshotsRetained[F[_]: Sync](spark: SparkSession): F[Option[Long]]
}

object Writer {

  /**
   * Sorted, so that two deployments' logs can be diffed. Values are quoted because some contain the
   * separator: `delta.dataSkippingStatsColumns` is a comma-separated column list.
   */
  def describeProperties(properties: Map[String, String]): String =
    if (properties.isEmpty)
      "none"
    else
      properties.toList.sorted.map { case (k, v) => s"$k=\"$v\"" }.mkString(", ")
}
