/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.tables

import cats.implicits._
import cats.effect.Sync
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.processing.SparkSchema

import scala.jdk.CollectionConverters._

/**
 * A base [[Writer]] for all flavours of Iceberg. Different concrete classes support different types
 * of catalog
 */
class IcebergWriter(config: Config.Iceberg) extends Writer {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  // The name is not important, outside of this app
  private final val sparkCatalog: String = "iceberg_catalog"

  override def sparkConfig: Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.debug.maxToStringFields" -> "65536",
      s"spark.sql.catalog.$sparkCatalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$sparkCatalog.io-impl" -> "org.apache.iceberg.io.ResolvingFileIO"
    ) ++ catalogConfig.map { case (k, v) =>
      s"spark.sql.catalog.$sparkCatalog.$k" -> v
    }

  override def prepareTable[F[_]: Sync](spark: SparkSession): F[Unit] =
    Logger[F].info(s"Creating Iceberg table $fqTable if it does not already exist...") >>
      Sync[F].blocking {
        spark.sql(s"""
          CREATE TABLE IF NOT EXISTS $fqTable
          (${SparkSchema.ddlForCreate})
          USING ICEBERG
          PARTITIONED BY (date(load_tstamp))
          TBLPROPERTIES($tableProps)
          $locationClause
        """)
      }.void *>
      // CREATE TABLE IF NOT EXISTS ignores TBLPROPERTIES for a table that already exists, so we also
      // apply them explicitly. Otherwise a table created by an older config never picks up new properties.
      Logger[F].info(s"Applying table properties to Iceberg table $fqTable...") *>
      Sync[F].blocking {
        if (config.icebergTableProperties.nonEmpty)
          spark.sql(s"ALTER TABLE $fqTable SET TBLPROPERTIES($tableProps)")
      }.void *>
      // We make an empty commit during startup, so the loader can fail early if we are missing any permissions
      write[F](spark.createDataFrame(List.empty[Row].asJava, SparkSchema.structForCreate))

  override def write[F[_]: Sync](df: DataFrame): F[Unit] =
    Sync[F].blocking {
      df.write
        .format("iceberg")
        .mode("append")
        .option("merge-schema", true)
        .option("check-ordering", false)
        .option("distribution-mode", "none")
        .saveAsTable(fqTable)
    }

  // Fully qualified table name
  private def fqTable: String =
    s"$sparkCatalog.`${config.database}`.`${config.table}`"

  private def locationClause: String =
    config.catalog match {
      case _: Config.IcebergCatalog.Hadoop =>
        // Hadoop catalog does not allow overriding path-based location
        ""
      case _ =>
        s"LOCATION '${config.location}'"
    }

  private def catalogConfig: Map[String, String] =
    config.catalog match {
      case c: Config.IcebergCatalog.Hadoop =>
        Map(
          "type" -> "hadoop",
          "warehouse" -> config.location.toString
        ) ++ c.options
      case c: Config.IcebergCatalog.BigLake =>
        Map(
          "catalog-impl" -> "org.apache.iceberg.gcp.biglake.BigLakeCatalog",
          "gcp_project" -> c.project,
          "gcp_location" -> c.region,
          "blms_catalog" -> c.name,
          "warehouse" -> config.location.toString
        ) ++ c.options
      case c: Config.IcebergCatalog.Glue =>
        Map(
          "catalog-impl" -> "org.apache.iceberg.aws.glue.GlueCatalog"
        ) ++ c.options
    }

  private def tableProps: String =
    config.icebergTableProperties
      .map { case (k, v) =>
        s"'$k'='$v'"
      }
      .mkString(", ")

  /**
   * Iceberg tolerates async deletes; in other words when we delete a file, there is no strong
   * requirement that the file must be deleted immediately. Iceberg uses unique file names and never
   * re-writes a file that was previously deleted
   */
  override def toleratesAsyncDelete: Boolean = true
}
