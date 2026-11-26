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

import cats.implicits._
import cats.effect.{Async, Sync}
import cats.effect.std.Mutex
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.lakes.{Alert, Config, DestinationSetupErrorCheck, RuntimeService}
import com.snowplowanalytics.snowplow.lakes.tables.Writer

/**
 * The operations that reach the lake's table, with retries, alerts and health reporting applied.
 *
 * `LakeWriter` delegates to this. Everything else it does is local to Spark, so nothing it raises
 * can be triaged as a customer setup error, and its success is no evidence that the destination is
 * reachable.
 *
 * @param mutexForRemoteWriting
 *   This mutex is needed because we allow overlapping windows. It prevents two different windows
 *   from writing to the table at the same time, and it is held across a window's whole retry
 *   sequence rather than per attempt. Two things depend on that. A retry's lookup of whether the
 *   failed attempt landed, which `IcebergWriter.lookUpWindow` explains. And a window that committed
 *   during an earlier one's backoff would land without being checkpointed, and be written again
 *   after a restart if the earlier window then ran out of retries.
 *
 * It does not order the windows: under lag, window N+1 can still take the lock before window N.
 *
 * It deliberately does not cover `prepareCommit`'s repartition and shuffle. Holding it over those
 * would leave the next window's `prepareCommit` queued behind this window's write with its staged
 * blocks still resident, which is the cost the split exists to avoid. So under the lag condition
 * described in CLAUDE.md's "Windows overlap" section, window N+1's shuffle can run concurrently
 * with window N's write. Nothing is staging while that is true: at most two windows are open, and
 * under lag those two are the ones finalizing.
 */
final class RemoteTable[F[_]: Async] private (
  underlying: RemoteTable.Attempts[F],
  appHealth: AppHealth.Interface[F, Alert, RuntimeService],
  retries: Config.Retries,
  destinationSetupErrorCheck: DestinationSetupErrorCheck,
  mutexForRemoteWriting: Mutex[F]
) {
  import RemoteTable.logger

  def create: F[Unit] =
    Retrying.withRetries(
      appHealth,
      retries.transientErrors,
      retries.setupErrors,
      RuntimeService.SparkWriter,
      Alert.FailedToCreateEventsTable,
      destinationSetupErrorCheck
    ) { _ =>
      underlying.create
    } <* appHealth.beHealthyForSetup

  /**
   * A second window's commit waits until this one has finished, including its retries.
   *
   * Checks the table's partitioning first, once. The check reads the partitioning this process's
   * previous commit left behind, so in steady state it makes no catalog call beyond loading the
   * table, which the commit needs anyway, and submits no Spark job. The trade-off is that a change
   * another process makes to the table is seen one commit late: the window committed just after the
   * change is written with packing that may not match the new spec, and the window after it fails
   * here before anything is written. One window of fragmented files is the cost accepted for
   * keeping a metadata round trip off every commit.
   *
   * Inside the mutex, because only there is the previous window's commit guaranteed to have
   * finished. Outside it, under lag, the check could read the state from two commits back. Before
   * the retries, because a mismatch cannot change between attempts and inside them would be
   * retried, alerted on and reported as unhealthy like a transient failure. Before the commit
   * rather than after it, so that a mismatch leaves the window unwritten as well as unacked: one
   * that was written but not acked would be loaded a second time once the table or config is fixed.
   */
  def commit(viewName: String): F[Unit] =
    mutexForRemoteWriting.lock.surround {
      checkPartitioning *>
        Retrying.withRetries(
          appHealth,
          retries.transientErrors,
          retries.setupErrors,
          RuntimeService.SparkWriter,
          Alert.FailedToCommitToLake,
          destinationSetupErrorCheck
        ) { attempt =>
          underlying.attemptCommit(viewName, isRetry = attempt > 0)
        }
    } <* appHealth.beHealthyForSetup

  /**
   * Raises if the table's partitioning no longer matches the config, and is meant to crash the
   * loader: the window is neither written nor acked. Both formats would go on writing correct data
   * under a changed spec, but `prepareCommit` packs for the configured one, so the files would
   * fragment.
   *
   * A failure to read the partitioning is logged and ignored, so the window is committed, as it is
   * for the table metrics. Only a partitioning that was read successfully can stop a window.
   */
  private def checkPartitioning: F[Unit] =
    underlying.checkPartitioning.recoverWith {
      case e: Writer.PartitioningMismatchException =>
        Sync[F].raiseError(e)
      case e =>
        Logger[F].warn(e)(
          "Could not read the table's partitioning to check it before committing. Committing the window anyway; the check runs again before the next commit"
        )
    }

  def getTableDataFilesTotal: F[Option[Long]] =
    underlying.getTableDataFilesTotal.handleErrorWith { e =>
      Logger[F].warn(e)("Failed to get table_data_files_total metric").as(None)
    }

  def getTableSnapshotsRetained: F[Option[Long]] =
    underlying.getTableSnapshotsRetained.handleErrorWith { e =>
      Logger[F].warn(e)("Failed to get table_snapshots_retained metric").as(None)
    }
}

object RemoteTable {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  /** The same operations as `RemoteTable`, each one a single attempt that raises what it hits */
  trait Attempts[F[_]] {

    /** Prepares the table, failing if its partitioning does not match the config */
    def create: F[Unit]

    /** Fails if the partitioning of the table already loaded does not match the config */
    def checkPartitioning: F[Unit]

    /**
     * @param isRetry
     *   Whether an earlier attempt at this window failed, and so might have landed all the same.
     */
    def attemptCommit(viewName: String, isRetry: Boolean): F[Unit]

    def getTableDataFilesTotal: F[Option[Long]]

    def getTableSnapshotsRetained: F[Option[Long]]
  }

  def build[F[_]: Async](
    underlying: Attempts[F],
    appHealth: AppHealth.Interface[F, Alert, RuntimeService],
    retries: Config.Retries,
    destinationSetupErrorCheck: DestinationSetupErrorCheck
  ): F[RemoteTable[F]] =
    Mutex[F].map(new RemoteTable(underlying, appHealth, retries, destinationSetupErrorCheck, _))
}
