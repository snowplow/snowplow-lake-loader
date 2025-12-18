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
import cats.data.NonEmptyList
import cats.effect.{Async, Resource, Sync}
import cats.effect.std.Mutex
import org.apache.spark.sql.{Row, SparkSession}
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
   * This is a lazy operation: The resulting union-ed DataFrame is not evaluated until the end of
   * the window when we commit to the lake.
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
}

object LakeWriter {

  trait WithHandledErrors[F[_]] extends LakeWriter[F]

  def build[F[_]: Async](
    config: Config.Spark,
    target: Config.Target
  ): Resource[F, LakeWriter[F]] = {
    val w = target match {
      case c: Config.Delta   => new DeltaWriter(c)
      case c: Config.Iceberg => new IcebergWriter(c)
    }
    for {
      session <- SparkUtils.session[F](config, w, target)
      writerParallelism = chooseWriterParallelism()
      mutex1 <- Resource.eval(Mutex[F])
      mutex2 <- Resource.eval(Mutex[F])
    } yield impl(session, w, writerParallelism, mutex1, mutex2)
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
      underlying
        .commit(viewName)
        .onError { case _ =>
          appHealth.beUnhealthyForRuntimeService(RuntimeService.SparkWriter)
        } <* appHealth.beHealthyForRuntimeService(RuntimeService.SparkWriter)
  }

  /**
   * Implementation of the LakeWriter
   *
   * @param mutexForRemoteWriting
   *   This mutex is needed because we allow overlapping windows. It prevents two different windows
   *   from trying to run the same expensive operation at the same time
   * @param mutexForLocalAppending
   *   This mutex is needed because `SparkUtils.localAppendRows` would otherwise have a race
   *   condition: It fetches a saved dataframe by name, modifies it, and re-saves the dataframe by
   *   the same name.
   */
  private def impl[F[_]: Sync](
    spark: SparkSession,
    w: Writer,
    writerParallelism: Int,
    mutexForRemoteWriting: Mutex[F],
    mutexForLocalAppending: Mutex[F]
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
      mutexForLocalAppending.lock.surround {
        SparkUtils.localAppendRows(spark, viewName, rows, schema)
      }

    def removeDataFrameFromDisk(viewName: String) =
      SparkUtils.dropView(spark, viewName)

    def commit(viewName: String): F[Unit] =
      for {
        df <- SparkUtils.prepareFinalDataFrame(spark, viewName, writerParallelism, w.expectsSortedDataframe)
        _ <- mutexForRemoteWriting.lock
               .surround {
                 w.write(df)
               }
      } yield ()
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
