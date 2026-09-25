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

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Mutex
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.lakes.{Alert, Config, DestinationSetupErrorCheck, RuntimeService}
import com.snowplowanalytics.snowplow.lakes.tables.{DeltaWriter, IcebergWriter, Writer}

trait LakeWriter[F[_]] {

  def createTable: F[Unit]

  /**
   * Creates an empty DataFrame with the atomic schema. Saves it with spark as a "view" so we can
   * refer to it by name later.
   *
   * Each window of events has its own DataFrame with unique name.
   */
  def initializeLocalDataFrame(viewName: String): F[Unit]

  /**
   * Append rows to the local DataFrame we are accumulating for this window
   *
   * The batch is encoded and checkpointed eagerly, so this runs work per call. It is only the union
   * with the accumulated view that stays lazy, and that is evaluated at the end of the window when
   * we commit to the lake.
   *
   * @param viewName
   *   Spark view for this window. The view should already be initialized before calling
   *   `localAppendRows`.
   * @param rows
   *   The new rows to append
   * @param schema
   *   The schema for this batch of rows. This might include new entities that are not already in
   *   the existing DataFrame.
   */
  def localAppendRows(
    viewName: String,
    rows: NonEmptyList[Row],
    schema: StructType
  ): F[Unit]

  /**
   * Un-saves the Spark view
   *
   * This allows Spark to clean up space when we are finished using a DataFrame. This must be called
   * at the end of each window.
   */
  def removeDataFrameFromDisk(viewName: String): F[Unit]

  /** Commit the DataFrame by writing it into the lake */
  def commit(viewName: String): F[Unit]

  /** Get the total number of active data files in the table */
  def getTableDataFilesTotal: F[Option[Long]]

  /** Get the total number of table snapshots/versions currently retained in the transaction log */
  def getTableSnapshotsRetained: F[Option[Long]]
}

object LakeWriter {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  trait WithHandledErrors[F[_]] extends LakeWriter[F]

  def build[F[_]: Async](
    config: Config.Spark,
    target: Config.Target,
    respectIgluNullability: Boolean
  ): Resource[F, LakeWriter[F]] = {
    val w = target match {
      case c: Config.Delta   => new DeltaWriter(c)
      case c: Config.Iceberg => new IcebergWriter(c)
    }
    // Delta writes all inner struct fields as nullable anyway, so correction is a no-op there.
    // If respectIgluNullability is disabled, all fields are nullable by schema design, so no correction needed.
    val shouldRestoreNullability = respectIgluNullability && !target.isInstanceOf[Config.Delta]
    for {
      session <- SparkUtils.session[F](config, w, target)
      stageOffHeap <- Resource.eval(SparkUtils.stageBatchesOffHeap[F](session))
      writerParallelism = chooseWriterParallelism()
      mutex1 <- Resource.eval(Mutex[F])
      mutex2 <- Resource.eval(Mutex[F])
      checkpointed <- Resource.eval(Ref[F].of(Map.empty[String, List[RDD[InternalRow]]]))
    } yield impl(session, w, writerParallelism, shouldRestoreNullability, stageOffHeap, mutex1, mutex2, checkpointed)
  }

  def withHandledErrors[F[_]: Async](
    underlying: LakeWriter[F],
    appHealth: AppHealth.Interface[F, Alert, RuntimeService],
    retries: Config.Retries,
    destinationSetupErrorCheck: DestinationSetupErrorCheck
  ): WithHandledErrors[F] = new WithHandledErrors[F] {
    def createTable: F[Unit] =
      Retrying.withRetries(
        appHealth,
        retries.transientErrors,
        retries.setupErrors,
        RuntimeService.SparkWriter,
        Alert.FailedToCreateEventsTable,
        destinationSetupErrorCheck
      ) { _ =>
        underlying.createTable
      } <* appHealth.beHealthyForSetup

    def initializeLocalDataFrame(viewName: String): F[Unit] =
      underlying.initializeLocalDataFrame(viewName)

    def localAppendRows(
      viewName: String,
      rows: NonEmptyList[Row],
      schema: StructType
    ): F[Unit] =
      underlying.localAppendRows(viewName, rows, schema)

    def removeDataFrameFromDisk(viewName: String): F[Unit] =
      underlying.removeDataFrameFromDisk(viewName)

    def commit(viewName: String): F[Unit] =
      Retrying.withRetries(
        appHealth,
        retries.transientErrors,
        retries.setupErrors,
        RuntimeService.SparkWriter,
        Alert.FailedToCommitToLake,
        destinationSetupErrorCheck
      ) { _ =>
        underlying.commit(viewName)
      } <* appHealth.beHealthyForSetup

    def getTableDataFilesTotal: F[Option[Long]] =
      underlying.getTableDataFilesTotal.handleErrorWith { e =>
        Logger[F].warn(e)("Failed to get table_data_files_total metric").as(None)
      }

    def getTableSnapshotsRetained: F[Option[Long]] =
      underlying.getTableSnapshotsRetained.handleErrorWith { e =>
        Logger[F].warn(e)("Failed to get table_snapshots_retained metric").as(None)
      }
  }

  /**
   * Implementation of the LakeWriter
   *
   * @param mutexForRemoteWriting
   *   This mutex is needed because we allow overlapping windows. It prevents two different windows
   *   from trying to run the same expensive operation at the same time
   * @param mutexForLocalAppending
   *   This mutex is needed because `SparkUtils.appendStagedBatch` would otherwise have a race
   *   condition: It fetches a saved dataframe by name, modifies it, and re-saves the dataframe by
   *   the same name. It deliberately covers neither `SparkUtils.encodeBatch` nor
   *   `SparkUtils.stageBatch`, which are the expensive steps and never touch the named view.
   * @param checkpointedRdds
   *   The checkpoint blocks accumulated by each open window, so they can be released as soon as the
   *   window is dropped rather than whenever Spark's `ContextCleaner` next gets to them. Keyed by
   *   view name, and the entry is removed when the window ends. What keeps this map bounded is that
   *   `Processing.manageDataFrame` brackets each window, so `removeDataFrameFromDisk` always runs,
   *   and that fs2 joins the `parEvalMapUnordered` fibers before running that finalizer, so no
   *   append can add an entry back after its window removed one. An entry that outlived its window
   *   would hold a strong reference to its RDDs for the life of the app, defeating the
   *   ContextCleaner as well, which is worse than not tracking them at all.
   */
  private def impl[F[_]: Sync](
    spark: SparkSession,
    w: Writer,
    writerParallelism: Int,
    shouldRestoreNullability: Boolean,
    stageOffHeap: Boolean,
    mutexForRemoteWriting: Mutex[F],
    mutexForLocalAppending: Mutex[F],
    checkpointedRdds: Ref[F, Map[String, List[RDD[InternalRow]]]]
  ): LakeWriter[F] = new LakeWriter[F] {
    def createTable: F[Unit] =
      w.prepareTable(spark)

    def initializeLocalDataFrame(viewName: String): F[Unit] =
      SparkUtils.initializeLocalDataFrame(spark, viewName)

    def localAppendRows(
      viewName: String,
      rows: NonEmptyList[Row],
      schema: StructType
    ): F[Unit] =
      for {
        // Encoding and staging are the expensive steps and need no exclusive access, so they stay
        // outside the mutex. Callers reach here from a `parEvalMapUnordered`, so batches encode and
        // stage in parallel and only the view read-modify-write is serialized.
        encoded <- SparkUtils.encodeBatch(spark, rows, schema)
        staged <- SparkUtils.stageBatch(spark, encoded, schema, stageOffHeap)
        // Recorded as soon as the batch is staged, which is when its blocks start existing, and
        // before the wait for the append mutex rather than after it. A batch cancelled between the
        // two stages blocks that never get recorded, and those are released by the ContextCleaner
        // exactly as they were before this map existed - the safe way to lose the race, and not
        // worth an `uncancelable` that would also make the staging job itself uninterruptible.
        _ <- checkpointedRdds.update(m => m.updated(viewName, staged.checkpointed :: m.getOrElse(viewName, Nil)))
        _ <- mutexForLocalAppending.lock.surround {
               SparkUtils.appendStagedBatch(spark, viewName, staged.df, schema, shouldRestoreNullability)
             }
      } yield ()

    def removeDataFrameFromDisk(viewName: String) =
      for {
        rdds <- checkpointedRdds.modify(m => (m - viewName, m.getOrElse(viewName, Nil)))
        _ <- SparkUtils.dropView(spark, viewName, rdds)
      } yield ()

    def commit(viewName: String): F[Unit] =
      for {
        df <- SparkUtils.prepareFinalDataFrame(spark, viewName, writerParallelism)
        _ <- mutexForRemoteWriting.lock
               .surround {
                 w.write(df)
               }
      } yield ()

    def getTableDataFilesTotal: F[Option[Long]] =
      w.getTableDataFilesTotal(spark)

    def getTableSnapshotsRetained: F[Option[Long]] =
      w.getTableSnapshotsRetained(spark)
  }

  /**
   * Allow spark to parallelize over _most_ of the available processors for writing to the lake,
   * because this speeds up how quickly we can sink a batch.
   *
   * But leave 1 processor always available, so that we are never blocked when trying to save one of
   * the intermediate dataframes.
   */
  private def chooseWriterParallelism(): Int =
    (Runtime.getRuntime.availableProcessors - 1).max(1)
}
