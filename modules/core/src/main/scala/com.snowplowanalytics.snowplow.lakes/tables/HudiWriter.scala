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
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.processing.SparkSchema

import scala.jdk.CollectionConverters._

class HudiWriter(config: Config.Hudi) extends Writer {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  override def sparkConfig: Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
    )

  override def prepareTable[F[_]: Sync](spark: SparkSession): F[Unit] = {
    val tableProps = config.hudiTableProperties
      .map { case (k, v) =>
        s"'$k'='$v'"
      }
      .mkString(", ")

    val internal_table_name = config.hudiTableProperties.get("hoodie.table.name").getOrElse("events")

    Logger[F].info(s"Creating Hudi table ${config.location} if it does not already exist...") >>
      maybeCreateDatabase[F](spark) *>
      Sync[F].blocking {
        spark.sql(s"""
          CREATE TABLE IF NOT EXISTS $internal_table_name
          (${SparkSchema.ddlForCreate})
          USING HUDI
          LOCATION '${config.location}'
          TBLPROPERTIES($tableProps)
        """): Unit

        // We call clean/archive during startup because it also triggers rollback of any previously
        // failed commits. We want to do the rollbacks before early, so that we are immediately
        // healthy once we start consuming events.
        spark.sql(s"""
          CALL run_clean(table => '$internal_table_name')
        """): Unit
        spark.sql(s"""
          CALL archive_commits(table => '$internal_table_name')
        """)
      }.void *>
      // We make an empty commit during startup, so the loader can fail early if we are missing any permissions
      write[F](spark.createDataFrame(List.empty[Row].asJava, SparkSchema.structForCreate))
  }

  private def maybeCreateDatabase[F[_]: Sync](spark: SparkSession): F[Unit] =
    config.hudiWriteOptions.get("hoodie.datasource.hive_sync.database") match {
      case Some(db) =>
        Sync[F].blocking {
          // This action does not have any effect beyond the internals of this loader.
          // It is required to prevent later exceptions for an unknown database.
          spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
        }.void
      case None =>
        Sync[F].unit
    }

  override def write[F[_]: Sync](df: DataFrame): F[Unit] =
    Sync[F].blocking {
      df.write
        .format("hudi")
        .mode("append")
        .options(config.hudiWriteOptions)
        .save(config.location.toString)
    }

  override def expectsSortedDataframe: Boolean = false
}
