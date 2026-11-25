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
import org.apache.iceberg.{HasTableOperations, Snapshot, Table}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.exceptions.CommitFailedException
import org.apache.iceberg.io.{FileIO, SupportsStorageCredentials}

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.processing.SparkSchema

import java.util.UUID
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

  override def prepareTable[F[_]: Sync](spark: SparkSession, processId: UUID): F[Unit] =
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
      checkVendedCredentials[F](spark) *>
      // We make an empty commit during startup, so the loader can fail early if we are missing any
      // permissions. It is tagged like a window, so that this process has a snapshot of its own
      // before its first window, for a retry's lookup to stop at. Nothing ever looks it up: a retried
      // `prepareTable` just makes the empty commit again.
      append[F](spark.createDataFrame(List.empty[Row].asJava, SparkSchema.structForCreate), Writer.WindowId.startup(processId))

  /**
   * A retry writes only when the window is not already in the table, because the attempt that
   * failed may have landed all the same: the catalog applies a commit and then the response is
   * lost, or Iceberg's own check of an unknown commit state fails. `landedAlready` has the lookup.
   * Every attempt tags its snapshot with the window's id, which is what the lookup finds.
   *
   * A lost response is only covered once the catalog has finished applying the commit. One still in
   * flight on the server when the retry looks, such as a slow catalog behind a client timeout, can
   * land after the lookup, and the window is then written twice.
   */
  override def write[F[_]: Sync](
    df: DataFrame,
    windowId: Writer.WindowId,
    isRetry: Boolean
  ): F[Unit] =
    if (isRetry)
      landedAlready[F](df.sparkSession, windowId).ifM(Sync[F].unit, append[F](df, windowId))
    else
      append[F](df, windowId)

  private def append[F[_]: Sync](df: DataFrame, windowId: Writer.WindowId): F[Unit] =
    Sync[F]
      .blocking {
        df.write
          .format("iceberg")
          .mode("append")
          .options(config.icebergWriteOptions)
          .option(s"snapshot-property.${IcebergWriter.windowIdProperty}", windowId.render)
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
        case e if isCommitFailure(e) => invalidateCachedTable(df.sparkSession, "a failed commit")
      }

  /**
   * Fails startup when the catalog options ask for vended credentials and the catalog loaded the
   * table without any.
   *
   * Databricks Unity Catalog does this when the loader may not reach the table's storage (a missing
   * EXTERNAL USE SCHEMA, or the metastore's external data access turned off). Its `loadTable` gives
   * no reason, and the empty startup commit then fails with a 500 that every set-up can return, so
   * the missing credentials are the only signal we can triage.
   *
   * Drops the cached table before failing. `CachingCatalog` would otherwise hand the retry the
   * table loaded here, with its empty credentials, even after the customer's fix.
   */
  private def checkVendedCredentials[F[_]: Sync](spark: SparkSession): F[Unit] =
    if (IcebergWriter.requestsVendedCredentials(config.catalog))
      Sync[F]
        .blocking(Spark3Util.loadIcebergTable(spark, fqTable).io())
        .flatMap { io =>
          if (IcebergWriter.noCredentialsVended(io))
            invalidateCachedTable(spark, "the catalog vended no storage credentials") *>
              Sync[F].raiseError(
                new IcebergWriter.NoVendedCredentialsError(s"REST catalog loaded $fqTable without vending storage credentials")
              )
          else
            Sync[F].unit
        }
    else
      Sync[F].unit

  /**
   * Whether an earlier attempt at this window landed: walks the ancestry of `main`, the branch the
   * loader writes to, from its head, and `IcebergWriter.lookUpWindow` has the rule for where the
   * walk stops. Anything short of finding the window's own snapshot counts as not landed, so the
   * window is written whenever there is doubt.
   *
   * Refreshes first, because the cached table predates the attempt that failed. Unlike
   * `invalidateCachedTable` this lets the refresh's errors through: an unreachable catalog fails
   * the attempt, and the next one comes back here rather than writing blind. Submits no Spark job,
   * so it needs no scheduler pool.
   */
  private def landedAlready[F[_]: Sync](spark: SparkSession, windowId: Writer.WindowId): F[Boolean] =
    for {
      _ <- Sync[F].blocking(spark.sql(s"REFRESH TABLE $fqTable"))
      table <- Sync[F].blocking(Spark3Util.loadIcebergTable(spark, fqTable))
      lookup <- Sync[F].blocking(IcebergWriter.lookUpWindow(IcebergWriter.ancestry(table), windowId))
      landed <- lookup match {
                  case IcebergWriter.Lookup.Landed(snapshotId) =>
                    Logger[F]
                      .warn(
                        s"Retrying window ${windowId.window}: found its snapshot $snapshotId already in $fqTable, so the failed attempt landed. Not writing it again."
                      )
                      .as(true)
                  case IcebergWriter.Lookup.ReachedOwnCommit(snapshotId, window) =>
                    Logger[F]
                      .info(
                        s"Retrying window ${windowId.window}: reached this process's snapshot $snapshotId of window $window without finding its own, so the failed attempt did not land. Writing it."
                      )
                      .as(false)
                  case IcebergWriter.Lookup.EndOfAncestry =>
                    Logger[F]
                      .info(
                        s"Retrying window ${windowId.window}: reached the end of the snapshots $fqTable retains without finding its own, so the failed attempt did not land. Writing it."
                      )
                      .as(false)
                }
    } yield landed

  private def isCommitFailure(t: Throwable): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ ne null).exists(_.isInstanceOf[CommitFailedException])

  /**
   * `REFRESH TABLE` reaches `SparkCatalog.invalidateTable`, which drops the `CachingCatalog` entry
   * and invalidates the catalog it wraps. It submits no Spark job, so it needs no scheduler pool.
   *
   * Handles its own errors: `onError` re-raises whatever its effect fails with, which would replace
   * the `CommitFailedException` the caller needs to classify. The same holds for the
   * `NoVendedCredentialsError` raised after it.
   */
  private def invalidateCachedTable[F[_]: Sync](spark: SparkSession, reason: String): F[Unit] =
    Logger[F].info(s"Dropping the cached metadata for $fqTable after $reason") *>
      Sync[F]
        .blocking(spark.sql(s"REFRESH TABLE $fqTable"))
        .void
        .handleErrorWith { e =>
          Logger[F].warn(e)(s"Could not refresh $fqTable after $reason")
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

object IcebergWriter {

  /** The snapshot summary property carrying `Writer.WindowId.render` */
  val windowIdProperty: String = "snowplow.lake-loader.window-id"

  sealed trait Lookup

  object Lookup {

    /** The window's own snapshot is in the table */
    final case class Landed(snapshotId: Long) extends Lookup

    /** The walk reached another of this process's commits first, so the window is not there */
    final case class ReachedOwnCommit(snapshotId: Long, window: String) extends Lookup

    /** The walk ran out of retained snapshots first, so the window is not there */
    case object EndOfAncestry extends Lookup
  }

  /**
   * Walks snapshots from the head of `main` towards its oldest retained ancestor, looking for the
   * window's own tag.
   *
   * Stops at the first snapshot tagged by this process, whatever its window. This process commits
   * one window at a time, with no other commit of its own between a window's attempts, so anything
   * of its own that is newer than the window's first attempt can only be the window itself. Older
   * than that, nothing can be. Snapshots from other processes, and untagged ones, are passed over.
   *
   * Stops on tags rather than on a timestamp: each writer stamps snapshots with its own clock, so
   * skew between pods could stop a timestamp walk short of the window and cause a duplicate.
   */
  def lookUpWindow(ancestry: Iterator[Snapshot], windowId: Writer.WindowId): Lookup =
    ancestry
      .map(s => (s.snapshotId, Option(s.summary).flatMap(m => Option(m.get(windowIdProperty))).flatMap(Writer.WindowId.parse)))
      .collectFirst {
        case (snapshotId, Some(found)) if found == windowId                     => Lookup.Landed(snapshotId)
        case (snapshotId, Some(found)) if found.processId == windowId.processId => Lookup.ReachedOwnCommit(snapshotId, found.window)
      }
      .getOrElse(Lookup.EndOfAncestry)

  /**
   * `main`'s head, then each parent in turn, ending where the parent has been expired or there is
   * none. Empty for a table with no snapshot.
   */
  def ancestry(table: Table): Iterator[Snapshot] =
    Iterator.unfold(Option(table.currentSnapshot)) {
      _.map { s =>
        (s, Option(s.parentId).flatMap(id => Option(table.snapshot(id.longValue))))
      }
    }

  /** The catalog loaded the table without the storage credentials its options asked it to vend */
  final class NoVendedCredentialsError(message: String) extends RuntimeException(message)

  private val headerOptionPrefix = "header."

  private val accessDelegationHeader = "X-Iceberg-Access-Delegation"

  /**
   * Whether the catalog options ask a REST catalog to vend storage credentials. The header may list
   * several delegation mechanisms, comma separated.
   *
   * Iceberg sends an option as a header only when its key starts with `header.` exactly, so the
   * prefix is matched as Iceberg matches it; the header name after it is case insensitive.
   */
  def requestsVendedCredentials(catalog: Config.IcebergCatalog): Boolean =
    catalog match {
      case c: Config.IcebergCatalog.Rest =>
        c.options.exists { case (k, v) =>
          k.startsWith(headerOptionPrefix) &&
          k.stripPrefix(headerOptionPrefix).equalsIgnoreCase(accessDelegationHeader) &&
          v.split(",").exists(_.trim === "vended-credentials")
        }
      case _ => false
    }

  /**
   * Whether a table's `FileIO` holds no vended storage credentials.
   *
   * Reads only the credentials a catalog vends as `storage-credentials` in its `loadTable`
   * response. Credentials vended through the response's `config` land in `FileIO.properties`
   * instead, and are not looked for: the REST catalogs the loader is deployed against (Databricks
   * Unity Catalog, BigLake, Snowflake Horizon) all vend as `storage-credentials`.
   *
   * A `FileIO` that cannot hold storage credentials gives no answer, so it never counts as empty.
   */
  def noCredentialsVended(io: FileIO): Boolean =
    io match {
      case s: SupportsStorageCredentials => s.credentials.isEmpty
      case _                             => false
    }
}
