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

import java.net.{InetAddress, UnknownHostException}
import java.util.UUID

import cats.implicits._
import cats.effect.Sync
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaConcurrentModificationException, DeltaLog}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.util.FileNames.DeltaFile
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

  /**
   * A partitioned table is partitioned by the generated `load_tstamp_date` and the configured
   * column. An unpartitioned one is created clustered by `load_tstamp`, without `load_tstamp_date`,
   * which exists only to partition by. The loader sets the clustering keys here, once, and never
   * checks or enforces them afterwards; `describeTable` only logs them.
   */
  override def prepareTable[F[_]: Sync](spark: SparkSession, processId: UUID): F[Unit] = {
    val unlaidOut = DeltaTable
      .createIfNotExists(spark)
      .location(config.location.toString)
      .tableName("events_internal_id") // The name does not matter

    val builder = config.partitionColumn match {
      case Some(column) => unlaidOut.partitionedBy("load_tstamp_date", column.name)
      case None         => unlaidOut.clusterBy("load_tstamp")
    }

    DeltaWriter.tableProperties(config.deltaTableProperties, expectedPartitioning).foreach { case (k, v) =>
      builder.property(k, v)
    }

    AtomicFields.withLoadTstamp.foreach(f => builder.addColumn(SparkSchema.asSparkField(f, true)))

    // This column needs special treatment because of the `generatedAlwaysAs` clause
    if (config.partitionColumn.isDefined)
      builder.addColumn {
        DeltaTable
          .columnBuilder(spark, "load_tstamp_date")
          .dataType("DATE")
          .generatedAlwaysAs("CAST(load_tstamp AS DATE)")
          .nullable(false)
          .build()
      }: Unit

    // hadoop-azure retries an unresolvable host for minutes before failing, so a wrong storage
    // account name is caught here first
    val checkHostname =
      if (List("abfs", "abfss").contains(config.location.getScheme)) {
        val host = config.location.getHost()
        Sync[F].blocking(InetAddress.getByName(host)).void.adaptError { case e: UnknownHostException =>
          new DeltaWriter.UnresolvedStorageHostException(host, e)
        }
      } else
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
          case e: DeltaAnalysisException if e.errorClass === Some("DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY") =>
            // Expected when the table was created by an older loader whose default table properties
            // differ from the current defaults, e.g. tables from before delta.deletedFileRetentionDuration
            // was added. An existing table keeps its properties. Delta only reaches this comparison
            // when the table's schema exactly matches the requested one: schema differences throw
            // DELTA_CREATE_TABLE_SCHEME_MISMATCH (ignored above) before properties are compared.
            Logger[F].debug(s"Caught and ignored DeltaAnalysisException")
          case e: DeltaAnalysisException if e.errorClass === Some("DELTA_CREATE_TABLE_WITH_DIFFERENT_PARTITIONING") =>
            // The table exists with partitioning other than the configured one. Delta compares this
            // before properties, and only when the schemas match. Ignored here so that the check
            // below reports it, rather than this being retried as transient.
            Logger[F].debug(s"Caught and ignored DeltaAnalysisException")
          case e: DeltaAnalysisException if e.errorClass === Some("DELTA_CREATE_TABLE_WITH_DIFFERENT_CLUSTERING") =>
            // The table's clustering keys differ from the ones the loader creates tables with. The
            // loader doesn't check clustering keys, only partitioning, below.
            Logger[F].debug(s"Caught and ignored DeltaAnalysisException")
          case e: DeltaAnalysisException if e.errorClass === Some("DELTA_CLUSTERING_REPLACE_TABLE_WITH_PARTITIONED_TABLE") =>
            // A partitioned config against a clustered table. Delta raises it before comparing
            // schemas. Ignored here so that the check below reports it, rather than this being
            // retried as transient.
            Logger[F].debug(s"Caught and ignored DeltaAnalysisException")
        } *>
      readPartitioning[F](spark, latest = true).flatMap(Writer.checkPartitioning[F](this, _))
  }

  override def tablePartitionColumn: Option[Config.PartitionColumn] = config.partitionColumn

  /**
   * Includes `load_tstamp_date`, which is the generated column that holds the date of
   * `load_tstamp`. Empty for an unpartitioned table, whatever its clustering keys, and including
   * one converted from partitioned that still has a `load_tstamp_date` column.
   */
  override def expectedPartitioning: Set[String] =
    config.partitionColumn.fold(Set.empty[String])(column => Set("load_tstamp_date", column.name))

  /**
   * `latest` takes `update`, as `describeTable` does. Otherwise `unsafeVolatileSnapshot`, as the
   * metric getters do: it is the snapshot of this process's last write or update, and so it sees
   * another process's change once this process next writes. Its metadata is normally already in
   * memory. It is not when `DeltaLog.forTable` misses its cache and loads the log afresh, or when
   * the snapshot has no usable checksum, and either can then submit a Spark job.
   */
  override def readPartitioning[F[_]: Sync](spark: SparkSession, latest: Boolean): F[Set[String]] =
    Sync[F].blocking {
      val deltaLog = DeltaLog.forTable(spark, config.location.toString)
      val snapshot = if (latest) deltaLog.update() else deltaLog.unsafeVolatileSnapshot
      snapshot.metadata.partitionColumns.toSet
    }

  /**
   * `update` rather than the `unsafeVolatileSnapshot` the metric getters read: a stale reading here
   * would be a wrong answer rather than a slightly old number.
   *
   * Every read is inside the `blocking`, because `Snapshot.protocol` and `Snapshot.metadata`
   * reconstruct themselves on first access - from the checksum file when there is a usable one, and
   * otherwise from a Spark job over `_delta_log`.
   */
  override def describeTable[F[_]: Sync](spark: SparkSession): F[List[String]] =
    Sync[F].blocking {
      val snapshot = DeltaLog.forTable(spark, config.location.toString).update()
      val protocol = snapshot.protocol
      val clustering = ClusteredTableUtils
        .getClusterBySpecOptional(snapshot)
        .fold("none")(_.columnNames.map(_.fieldNames.mkString(".")).mkString("[", ", ", "]"))
      List(
        s"Delta table ${config.location}: partitioned by ${snapshot.metadata.partitionColumns.mkString("[", ", ", "]")}, " +
          s"clustered by $clustering, " +
          s"min reader version = ${protocol.minReaderVersion}, min writer version = ${protocol.minWriterVersion}, " +
          s"table features = ${describeTableFeatures(protocol)}",
        s"Delta table properties: ${Writer.describeProperties(snapshot.metadata.configuration)}"
      )
    }

  /**
   * Above the legacy protocol versions Delta records capabilities as named features rather than as
   * a version bump. `implicitlyAndExplicitlySupportedFeatures` rather than
   * `readerAndWriterFeatureNames`, which holds only the named ones and so is empty for a legacy
   * protocol - the table this reports on is often one an older loader created.
   */
  private def describeTableFeatures(protocol: Protocol): String = {
    val names = protocol.implicitlyAndExplicitlySupportedFeatures.map(_.name)
    if (names.isEmpty) "none" else names.toList.sorted.mkString("[", ", ", "]")
  }

  /**
   * Sink to delta with retries
   *
   * Retry is needed if a concurrent writer updated the table metadata. It is only needed during
   * schema evolution, when the pipeine starts tracking a new schema for the first time.
   *
   * Retry happens immediately with no delay. For this type of exception there is no reason to
   * delay.
   *
   * Ignores `windowId` and `isRetry`, so a retried commit that had in fact landed writes the window
   * again.
   */
  override def write[F[_]: Sync](
    df: DataFrame,
    windowId: Writer.WindowId,
    isRetry: Boolean
  ): F[Unit] =
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

  override def getTableDataFilesTotal[F[_]: Sync](spark: SparkSession): F[Option[Long]] =
    for {
      deltaLog <- Sync[F].blocking(DeltaLog.forTable(spark, config.location.toString))
      snapshot = deltaLog.unsafeVolatileSnapshot
      total <- Sync[F].delay(snapshot.checksumOpt.map(_.numFiles))
    } yield total

  override def getTableSnapshotsRetained[F[_]: Sync](spark: SparkSession): F[Option[Long]] =
    for {
      deltaLog <- Sync[F].blocking(DeltaLog.forTable(spark, config.location.toString))
      snapshot       = deltaLog.unsafeVolatileSnapshot
      currentVersion = snapshot.version
      // Lists the whole `_delta_log` directory without reading any file contents.
      // `LogStore.listFrom` materializes the listing into an array and sorts it, so the directory is
      // read in full before `collectFirst` takes an element, and the IO grows with the retained log.
      earliestVersionOpt <- Sync[F].blocking {
                              deltaLog.store
                                .listFrom(
                                  path       = FileNames.listingPrefix(deltaLog.logPath, 0),
                                  hadoopConf = deltaLog.newDeltaHadoopConf()
                                )
                                .collectFirst { case DeltaFile(_, version) => version }
                            }
      count = earliestVersionOpt.map { earliestVersion =>
                currentVersion - earliestVersion + 1
              }
    } yield count
}

object DeltaWriter {

  private val statsColumnsProperty = "delta.dataSkippingStatsColumns"

  /**
   * The configured properties, less any partition column in `delta.dataSkippingStatsColumns`: Delta
   * refuses to create a table partitioned by a column it collects stats on. So a clustered table
   * keeps every listed column, which it needs for any of them to become a clustering key later.
   * When the property is absent Delta's own default applies, which is the first 32 columns. Names
   * are compared case-insensitively, as Delta resolves them.
   */
  def tableProperties(configured: Map[String, String], partitionColumns: Set[String]): Map[String, String] =
    configured.get(statsColumnsProperty) match {
      case Some(list) =>
        val columns  = list.split(",").toList
        val excluded = partitionColumns.map(_.toLowerCase)
        val kept     = columns.filterNot(c => excluded.contains(c.trim.stripPrefix("`").stripSuffix("`").toLowerCase))
        if (kept.size === columns.size) configured else configured.updated(statsColumnsProperty, kept.mkString(","))
      case None =>
        configured
    }

  /**
   * The host of an ABFS table location does not resolve, e.g. a wrong storage account name
   *
   * Thrown by the host check in `prepareTable` in place of the bare `UnknownHostException`, which
   * s3a and Iceberg REST catalogs also throw. `DestinationSetupError` must not match other stores'
   * exceptions, so this type is what lets `DestinationSetupError.Azure` recognise the failure.
   */
  final class UnresolvedStorageHostException(val host: String, cause: UnknownHostException)
      extends Exception(s"Storage host $host does not resolve", cause)
}
