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

  /** A second window's commit waits until this one has finished, including its retries. */
  def commit(viewName: String): F[Unit] =
    mutexForRemoteWriting.lock.surround {
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

    def create: F[Unit]

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
