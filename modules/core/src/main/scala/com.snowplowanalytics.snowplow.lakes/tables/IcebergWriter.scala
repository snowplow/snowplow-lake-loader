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
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.iceberg.{HasTableOperations, Table}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.exceptions.CommitFailedException

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
          PARTITIONED BY (date(load_tstamp), event_name)
          TBLPROPERTIES($tableProps)
          $locationClause
        """)
      }.void *>
      // We make an empty commit during startup, so the loader can fail early if we are missing any permissions
      write[F](spark.createDataFrame(List.empty[Row].asJava, SparkSchema.structForCreate))

  override def write[F[_]: Sync](df: DataFrame): F[Unit] =
    Sync[F]
      .blocking {
        df.write
          .format("iceberg")
          .mode("append")
          .options(config.icebergWriteOptions)
          .saveAsTable(fqTable)
      }
      // This exception happens when the loader loses a race to evolve the table's schema: a new
      // Iglu schema reaches every loader in the deployment within the same window, so they all add
      // the same columns and all but one lose. This path is therefore as rare as schema evolution.
      // Appends race far more often and rarely arrive here, because `SnapshotProducer` retries
      // those in a loop of its own.
      //
      // We must drop the cached table, or the caller's retry cannot succeed. `SparkCatalog` installs
      // a `CachingCatalog`, so a retry resolves the same table, and no `TableOperations` refreshes
      // after a failed commit - leaving the retry to re-send a commit built on metadata the catalog
      // has already rejected.
      //
      // Restricted to this exception, so a catalog erroring at every loader does not also take a
      // table load from each of their retries. Not narrowed further: the message comes from the
      // catalog server, and telling the schema commit from the append means matching on Spark write
      // builder internals. Matched anywhere in the causal chain, because `V2TableWriteExec` wraps
      // the commit's exception when the abort that follows it also fails.
      .onError {
        case e if isCommitFailure(e) => invalidateCachedTable(df.sparkSession)
      }

  private def isCommitFailure(t: Throwable): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ ne null).exists(_.isInstanceOf[CommitFailedException])

  /**
   * `REFRESH TABLE` reaches `SparkCatalog.invalidateTable`, which drops the `CachingCatalog` entry
   * and invalidates the catalog it wraps. It submits no Spark job, so it needs no scheduler pool.
   *
   * Handles its own errors: `onError` re-raises whatever its effect fails with, which would replace
   * the `CommitFailedException` the caller needs to classify.
   */
  private def invalidateCachedTable[F[_]: Sync](spark: SparkSession): F[Unit] =
    Logger[F].info(s"Dropping the cached metadata for $fqTable after a failed commit") *>
      Sync[F]
        .blocking(spark.sql(s"REFRESH TABLE $fqTable"))
        .void
        .handleErrorWith { e =>
          Logger[F].warn(e)(s"Could not refresh $fqTable after a failed commit")
        }

  // Fully qualified table name
  private def fqTable: String =
    s"$sparkCatalog.`${config.database}`.`${config.table}`"

  private def locationClause: String =
    (config.catalog, config.location) match {
      case (_: Config.IcebergCatalog.Hadoop, _) =>
        // Hadoop catalog does not allow overriding path-based location
        ""
      case (_, None) =>
        // Some Iceberg catalogs provide the location without needing it in the config file
        ""
      case (_, Some(location)) =>
        s"LOCATION '$location'"
    }

  private def catalogConfig: Map[String, String] =
    config.catalog match {
      case c: Config.IcebergCatalog.Hadoop =>
        Map(
          "type" -> "hadoop"
        ) ++ config.location.map(uri => "warehouse" -> uri.toString).toMap ++ c.options
      case c: Config.IcebergCatalog.Glue =>
        Map(
          "catalog-impl" -> "org.apache.iceberg.aws.glue.GlueCatalog"
        ) ++ c.options
      case c: Config.IcebergCatalog.Rest =>
        Map(
          "catalog-impl" -> "org.apache.iceberg.rest.RESTCatalog",
          "uri" -> c.uri.toString,
          "warehouse" -> c.name
        ) ++ c.options
    }

  private def tableProps: String =
    config.icebergTableProperties
      .map { case (k, v) =>
        s"'$k'='$v'"
      }
      .mkString(", ")

  override def describeTable[F[_]: Sync](spark: SparkSession): F[List[String]] =
    Sync[F].blocking {
      val table = Spark3Util.loadIcebergTable(spark, fqTable)
      List(
        s"Iceberg table $fqTable: location = ${table.location}, format version = ${formatVersion(table)}, " +
          s"partitioned by ${partitionSpec(table)}, sort order = ${sortOrder(table)}",
        s"Iceberg table properties: ${Writer.describeProperties(table.properties.asScala.toMap)}"
      )
    }

  /**
   * A partition field's name need not name either its transform or its source column: `bucket(16,
   * event_id) AS shard` is just `shard`.
   */
  private def partitionSpec(table: Table): String =
    table.spec.fields.asScala
      .map(field => s"${field.transform}(${table.spec.schema.findColumnName(field.sourceId)}) AS ${field.name}")
      .mkString("[", ", ", "]")

  /**
   * The format version is a field of `TableMetadata` rather than a table property, so
   * `Table.properties` does not carry it. `current` refreshes nothing.
   *
   * Matched rather than cast, because `Table` does not declare `operations`.
   */
  private def formatVersion(table: Table): String =
    table match {
      case hasOps: HasTableOperations => hasOps.operations.current.formatVersion.toString
      case _                          => "unknown"
    }

  /**
   * `SortField` carries a source id rather than a name, and a transform that its direction hides.
   */
  private def sortOrder(table: Table): String =
    if (table.sortOrder.isUnsorted)
      "unsorted"
    else
      table.sortOrder.fields.asScala
        .map { field =>
          s"${field.transform}(${table.sortOrder.schema.findColumnName(field.sourceId)}) ${field.direction} ${field.nullOrder}"
        }
        .mkString("[", ", ", "]")

  def getTableDataFilesTotal[F[_]: Sync](spark: SparkSession): F[Option[Long]] =
    for {
      icebergTable <- Sync[F].blocking(Spark3Util.loadIcebergTable(spark, fqTable))
      snapshot <- Sync[F].delay(icebergTable.currentSnapshot())
      total <- Sync[F].delay(snapshot.summary().get("total-data-files").toLong)
    } yield Some(total)

  /**
   * Counts the snapshots Iceberg is retaining, for the `table_snapshots_retained` gauge.
   *
   * Not the `.snapshots` metadata table: `SnapshotsTable` builds its rows from this same
   * `Table.snapshots()` call, so querying it submits a Spark job for an answer already in memory.
   */
  def getTableSnapshotsRetained[F[_]: Sync](spark: SparkSession): F[Option[Long]] =
    for {
      icebergTable <- Sync[F].blocking(Spark3Util.loadIcebergTable(spark, fqTable))
      retained <- Sync[F].delay(icebergTable.snapshots().asScala.size.toLong)
    } yield Some(retained)
}
