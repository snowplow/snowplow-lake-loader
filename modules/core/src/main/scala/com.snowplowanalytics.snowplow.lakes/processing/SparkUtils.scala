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

import cats.data.NonEmptyList
import cats.effect.{Async, Sync}
import cats.effect.kernel.Resource
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.tables.Writer
import com.snowplowanalytics.snowplow.lakes.fs.LakeLoaderFileSystem

import scala.jdk.CollectionConverters._

private[processing] object SparkUtils {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def session[F[_]: Async](
    config: Config.Spark,
    writer: Writer,
    target: Config.Target
  ): Resource[F, SparkSession] = {
    val builder =
      SparkSession
        .builder()
        .appName("snowplow-lake-loader")
        .master(s"local[*, ${config.taskRetries}]")
        .config(sparkConfigOptions(config, writer))

    val openLogF  = Logger[F].info("Creating the global spark session...")
    val closeLogF = Logger[F].info("Closing the global spark session...")
    val buildF    = Sync[F].delay(builder.getOrCreate())

    Resource
      .make(openLogF >> buildF)(s => closeLogF >> Sync[F].blocking(s.close()))
      .evalTap { session =>
        target match {
          case delta: Config.Delta =>
            Sync[F].delay {
              // Forces Spark to use `LakeLoaderFileSystem` when writing to the Lake via Hadoop
              // Delta tolerates async deletes; in other words when we delete a file, there is no strong
              // requirement that the file must be deleted immediately. Delta uses unique file names and never
              // re-writes a file that was previously deleted
              LakeLoaderFileSystem.overrideHadoopFileSystemConf(delta.location, session.sparkContext.hadoopConfiguration)
            }
          case _ => Sync[F].unit
        }
      }
  }

  private def sparkConfigOptions(config: Config.Spark, writer: Writer): Map[String, String] = {
    val gcpUserAgentKey   = "fs.gs.storage.http.headers.user-agent"
    val gcpUserAgentValue = s"${config.gcpUserAgent.productName}/lake-loader (GPN:Snowplow;)"
    writer.sparkConfig ++ config.conf + (gcpUserAgentKey -> gcpUserAgentValue)
  }

  def initializeLocalDataFrame[F[_]: Sync](spark: SparkSession, viewName: String): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Initializing local DataFrame with name $viewName")
      _ <- Sync[F].blocking {
             try {
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
               spark.emptyDataFrame.createTempView(viewName)
             } finally
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
           }
    } yield ()

  def localAppendRows[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    rows: NonEmptyList[Row],
    schema: StructType
  ): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Saving batch of ${rows.size} events to local DataFrame $viewName")
      _ <- Sync[F].blocking {
             try {
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
               spark
                 .createDataFrame(rows.toList.asJava, schema)
                 .coalesce(1)
                 .localCheckpoint()
                 .unionByName(spark.table(viewName), allowMissingColumns = true)
                 .createOrReplaceTempView(viewName)
             } finally
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
           }
    } yield ()

  def prepareFinalDataFrame[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    writerParallelism: Int,
    writerExpectsSortedDataframe: Boolean
  ): F[DataFrame] =
    for {
      df <- Sync[F].pure(spark.table(viewName))
      df <- Sync[F].pure {
              // Create equally-balanced partitions, for which events with similar event_name are likely to be in the same partition.
              // This maximizes output file sizes, for a lake which is partitioned by event_name.
              if (writerParallelism > 1) df.repartitionByRange(writerParallelism, col("event_name"), col("event_id")) else df.coalesce(1)
            }
      df <- Sync[F].pure(if (writerExpectsSortedDataframe) df.sortWithinPartitions("event_name") else df)
    } yield df.withColumn("load_tstamp", current_timestamp())

  def dropView[F[_]: Sync](spark: SparkSession, viewName: String): F[Unit] =
    Logger[F].info(s"Removing Spark data frame $viewName from local disk...") >>
      Sync[F].blocking {
        try {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
          spark.catalog.dropTempView(viewName)
        } finally
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
      }.void
}
