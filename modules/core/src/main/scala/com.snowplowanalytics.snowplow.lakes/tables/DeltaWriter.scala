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

import java.net.InetAddress

import cats.implicits._
import cats.effect.Sync
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaConcurrentModificationException}
import io.delta.tables.DeltaTable

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.processing.SparkSchema
import com.snowplowanalytics.snowplow.loaders.transform.AtomicFields

class DeltaWriter(config: Config.Delta) extends Writer {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  override def sparkConfig: Map[String, String] =
    Map(
      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

  override def prepareTable[F[_]: Sync](spark: SparkSession): F[Unit] = {
    val builder = DeltaTable
      .createIfNotExists(spark)
      .partitionedBy("load_tstamp_date", "event_name")
      .location(config.location.toString)
      .tableName("events_internal_id") // The name does not matter

    config.deltaTableProperties.foreach { case (k, v) =>
      builder.property(k, v)
    }

    AtomicFields.withLoadTstamp.foreach(f => builder.addColumn(SparkSchema.asSparkField(f, true)))

    // This column needs special treatment because of the `generatedAlwaysAs` clause
    builder.addColumn {
      DeltaTable
        .columnBuilder(spark, "load_tstamp_date")
        .dataType("DATE")
        .generatedAlwaysAs("CAST(load_tstamp AS DATE)")
        .nullable(false)
        .build()
    }: Unit

    // For Azure a wrong storage name means an invalid hostname and infinite retries when creating the Delta table
    // If the hostname is invalid, UnknownHostException gets thrown
    val checkHostname =
      if (List("abfs", "abfss").contains(config.location.getScheme))
        Sync[F].blocking(InetAddress.getByName(config.location.getHost()))
      else
        Sync[F].unit

    Logger[F].info(s"Creating Delta table ${config.location} if it does not already exist...") >>
      checkHostname >>
      Sync[F]
        .blocking(builder.execute())
        .void
        .recoverWith {
          case e: DeltaAnalysisException if e.errorClass === Some("DELTA_CREATE_TABLE_SCHEME_MISMATCH") =>
            // Expected when table exists and contains some unstruct_event or context columns
            Logger[F].debug(s"Caught and ignored DeltaAnalysisException")
        }
  }

  /**
   * Sink to delta with retries
   *
   * Retry is needed if a concurrent writer updated the table metadata. It is only needed during
   * schema evolution, when the pipeine starts tracking a new schema for the first time.
   *
   * Retry happens immediately with no delay. For this type of exception there is no reason to
   * delay.
   */
  override def write[F[_]: Sync](df: DataFrame): F[Unit] =
    Sync[F].untilDefinedM {
      Sync[F]
        .blocking[Option[Unit]] {
          df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", true)
            .save(config.location.toString)
          Some(())
        }
        .recoverWith { case e: DeltaConcurrentModificationException =>
          Logger[F]
            .warn(s"Retryable error writing to delta table: DeltaConcurrentModificationException with conflict type ${e.conflictType}")
            .as(None)
        }
    }

  override def expectsSortedDataframe: Boolean = false
}
