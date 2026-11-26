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

import cats.implicits._
import cats.effect.Sync
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.snowplowanalytics.snowplow.lakes.Config

import java.util.UUID

/** The methods needed for a writing specific table format (e.g. delta or iceberg) */
trait Writer {

  /** Spark config parameters which the Lake Loader needs for this specific table format */
  def sparkConfig: Map[String, String]

  /**
   * Prepare a table to be ready for loading. Runs once when the app first starts up.
   *
   * For some table formats that can mean creating the table, or registering it in an external
   * catalog.
   *
   * Raises `Writer.PartitioningMismatchException` if the table's partitioning is not
   * `expectedPartitioning`, before writing anything to the table.
   *
   * @param processId
   *   Identifies this run of the loader.
   */
  def prepareTable[F[_]: Sync](spark: SparkSession, processId: UUID): F[Unit]

  /** The column the table is partitioned by, alongside the date of `load_tstamp` */
  def tablePartitionColumn: Option[Config.PartitionColumn]

  /**
   * The partitioning a table must have for this loader's config, in `readPartitioning`'s terms.
   * Empty for an unpartitioned table.
   */
  def expectedPartitioning: Set[String]

  /**
   * The table's actual partitioning, one element per partition field, in terms that
   * `expectedPartitioning` can be compared with. A set, because the order of the fields does not
   * change which rows share a file.
   *
   * @param latest
   *   Whether the read must see the table as it is now, even if that submits a Spark job. Otherwise
   *   it reflects this process's most recent commit, or anything newer it happens to have loaded.
   *   It then makes no catalog round trip of its own beyond loading the table, and normally submits
   *   no Spark job, so a change another process made since then is not seen until this process
   *   commits again.
   */
  def readPartitioning[F[_]: Sync](spark: SparkSession, latest: Boolean): F[Set[String]]

  /**
   * Write Snowplow events into the table.
   *
   * For a partitioned table, both formats sort each writer partition by the table's partition
   * columns and keep one output file open at a time, reached differently: Iceberg from
   * `fanout-enabled` being false in `icebergWriteOptions`, Delta from Spark's `FileFormatWriter`,
   * whose concurrent-writer path needs `spark.sql.maxConcurrentOutputFileWriters`. The alternative
   * in both is a writer that keeps a file open per partition value the task meets, and an open
   * parquet writer holds its column dictionaries and buffers on the JVM heap, so heap residency
   * would scale with the distinct partition values in a task rather than with anything the loader
   * sizes. Those values reduce to the task's distinct `event_name`s, or `app_id`s when that is the
   * partition column: Spark's `ComputeCurrentTime` folds the `current_timestamp()` that
   * `SparkUtils.readFinalDataFrame` assigns to `load_tstamp` into one literal for the whole write,
   * so the date half of the partition spec is constant.
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
   *
   * An unpartitioned Delta table gives Spark no required ordering to impose, so its commit does not
   * sort at all: each writer task writes a single file, holding the rows that hashed to it by
   * `event_id` (see `SparkUtils.prepareFinalDataFrame`) in shuffle order. This holds for a
   * clustered table too, because Delta clusters only when the table is optimized, not on an append.
   * The commit then draws no execution memory for a sort, and the rows within a file are not
   * grouped by any column until the table is optimized, which is the trade accepted here.
   *
   * @param windowId
   *   Identifies the window being written; the same on every attempt at it.
   * @param isRetry
   *   Whether an earlier attempt at this window failed, and so may have landed all the same.
   */
  def write[F[_]: Sync](
    df: DataFrame,
    windowId: Writer.WindowId,
    isRetry: Boolean
  ): F[Unit]

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
   * Raised when the table's partitioning is not the one the config asks for. Its message carries
   * the expected and actual partitioning.
   *
   * @param column
   *   The configured partition column, which `expected` is derived from
   */
  final class PartitioningMismatchException(
    val column: Option[Config.PartitionColumn],
    val expected: Set[String],
    val actual: Set[String]
  ) extends RuntimeException(
        s"The table is partitioned by ${describePartitioning(actual)}, but the configured partitionColumn needs ${describeExpected(expected)}"
      )

  def checkPartitioning[F[_]: Sync](writer: Writer, actual: Set[String]): F[Unit] =
    Sync[F].raiseUnless(actual === writer.expectedPartitioning)(
      new PartitioningMismatchException(writer.tablePartitionColumn, writer.expectedPartitioning, actual)
    )

  private def describePartitioning(fields: Set[String]): String =
    fields.toList.sorted.mkString("[", ", ", "]")

  private def describeExpected(fields: Set[String]): String =
    if (fields.isEmpty) "no partitioning" else describePartitioning(fields)

  /**
   * Identifies one window's commit, so that a retry can tell whether an earlier attempt landed.
   *
   * `processId` is drawn once per process, so the id is stable across a window's attempts and
   * unique across pods and restarts. It does not survive a restart: a window re-consumed after one
   * is written again.
   *
   * @param window
   *   The window's view name, or `startupWindow` for the empty commit `prepareTable` makes.
   */
  final case class WindowId(processId: UUID, window: String) {
    def render: String = s"$processId/$window"
  }

  object WindowId {

    /** Cannot collide with a window's view name, which is `v` followed by digits. */
    val startupWindow: String = "startup"

    def startup(processId: UUID): WindowId = WindowId(processId, startupWindow)

    /** `None` for anything `render` did not produce, such as a tag written by some other tool. */
    def parse(rendered: String): Option[WindowId] =
      rendered.split("/", 2) match {
        case Array(processId, window) if window.nonEmpty =>
          scala.util.Try(UUID.fromString(processId)).toOption.map(WindowId(_, window))
        case _ =>
          None
      }
  }

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
