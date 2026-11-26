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
import cats.effect.{Deferred, IO, Resource}
import cats.effect.testing.specs2.CatsEffect
import cats.implicits._
import fs2.io.file.Path
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.spark.sql.{DataFrame, Row, SnowplowSparkBlockProbe, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import io.delta.tables.DeltaTable
import org.specs2.Specification

import java.io.File
import java.sql.Timestamp
import java.util.UUID

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.lakes.{
  Alert,
  AnyConfig,
  Config,
  DestinationSetupError,
  RuntimeService,
  TableFormatSetupError,
  TestConfig
}
import com.snowplowanalytics.snowplow.lakes.tables.{DeltaWriter, IcebergWriter, Writer}
import com.snowplowanalytics.snowplow.loaders.transform.AtomicFields

/**
 * The configurable `partitionColumn`, for both formats: the table it creates, the packing over its
 * values, and the checks that the table's partitioning still matches it.
 *
 * The partitioned examples configure `app_id`. The default is `event_name`, and every other spec
 * runs with it, including the ones where a table the loader created passes its own startup check.
 * The unpartitioned examples are Delta only, the one format that accepts a `null`
 * `partitionColumn`.
 */
class PartitionColumnSpec extends Specification with CatsEffect {
  import PartitionColumnSpec._

  override val Timeout = 180.seconds

  def is = sequential ^ s2"""
  With app_id as the partition column, a new table should be partitioned by the date and app_id:
    Delta $delta1
    Iceberg $iceberg1

  With app_id as the partition column, a window mixing null and non-null app_ids should be written, sorted, with as many files per app_id as the packer gave it partitions:
    Delta $delta2
    Iceberg $iceberg2
    Delta with Spark's concurrent writers, which do not sort, as the control that the measure of the sort reads 0 without one $delta2Unsorted

  A table partitioned other than the config says should fail startup as a setup error, without writing to the table:
    Delta $delta3
    Iceberg $iceberg3

  A table whose partitioning changes mid-run should fail a commit, before writing it, no later than the second window after the change:
    Delta, changed through the loader's own session $delta4
    Iceberg, changed through a catalog of its own, which the loader's cached table does not see $iceberg4

  A new Delta table should leave its own partition column out of the stats columns, and keep the other:
    event_name $deltaStats1
    app_id $deltaStats2

  With partitionColumn null, a new Delta table should be unpartitioned, clustered by load_tstamp, without load_tstamp_date, with stats on event_name and app_id, at reader version 1 $unpartitioned1

  With partitionColumn null, the loader should start against:
    a table whose clustering keys were changed, its schema otherwise identical $unpartitioned2
    a table whose clustering keys were changed after a window added entity columns $unpartitioned3
    an unpartitioned table that keeps a generated load_tstamp_date, and fill that column in when it writes $unpartitioned4
    an unpartitioned table with no clustering $unpartitioned8
    a table whose clustering was removed with CLUSTER BY NONE $unpartitioned9

  A layout mismatch at startup should be a setup error with the alert for the configured layout:
    partitionColumn null against a partitioned table $unpartitioned5
    partitionColumn event_name against a clustered table $unpartitioned6

  With partitionColumn null, a window should be spread by a hash of event_id, committed without a sort, and land in the unpartitioned, clustered table $unpartitioned7
  """

  def delta1   = newTable(TestConfig.Delta, Set("load_tstamp_date", "app_id"))
  def iceberg1 = newTable(TestConfig.Iceberg, Set("day(load_tstamp)", "identity(app_id)"))

  def delta2         = packing(TestConfig.Delta, concurrentWriters = false)
  def iceberg2       = packing(TestConfig.Iceberg, concurrentWriters = false)
  def delta2Unsorted = packing(TestConfig.Delta, concurrentWriters = true)

  def delta3   = startupMismatch(TestConfig.Delta)
  def iceberg3 = startupMismatch(TestConfig.Iceberg)

  def delta4   = midRunMismatch(TestConfig.Delta)
  def iceberg4 = midRunMismatch(TestConfig.Iceberg)

  def deltaStats1 = deltaStats(Config.PartitionColumn.EventName)
  def deltaStats2 = deltaStats(Config.PartitionColumn.AppId)

  private def deltaStats(column: Config.PartitionColumn) = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = withPartitionColumn(TestConfig.defaults(TestConfig.Delta, tmpDir), Some(column))
    val other  = Config.PartitionColumn.all.filterNot(_ === column).map(_.name)
    val writer = writerFor(config.output.good)
    SparkUtils.session[IO](config.spark, writer, config.output.good, cores = 2).use { spark =>
      for {
        _ <- writer.prepareTable[IO](spark, UUID.randomUUID)
        snapshot <- IO.blocking(deltaSnapshot(spark, config.output.good))
      } yield List(
        snapshot.metadata.partitionColumns.toSet must beEqualTo(Set("load_tstamp_date", column.name)),
        statsColumns(snapshot) must not(contain(column.name)),
        statsColumns(snapshot) must containAllOf(other)
      ).reduce(_ and _)
    }
  }

  def unpartitioned1 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = unpartitionedDelta(tmpDir)
    val writer = writerFor(config.output.good)
    SparkUtils.session[IO](config.spark, writer, config.output.good, cores = 2).use { spark =>
      for {
        _ <- writer.prepareTable[IO](spark, UUID.randomUUID)
        snapshot <- IO.blocking(deltaSnapshot(spark, config.output.good))
        described <- writer.describeTable[IO](spark)
      } yield List(
        snapshot.metadata.partitionColumns must beEmpty,
        clusteringDomain(snapshot) must beSome(contain("[[\"load_tstamp\"]]")),
        snapshot.metadata.schema.fieldNames.toList must not(contain("load_tstamp_date")),
        statsColumns(snapshot) must containAllOf(List("event_name", "app_id")),
        snapshot.protocol.minReaderVersion must beEqualTo(1),
        described.head must contain("clustered by [load_tstamp]")
      ).reduce(_ and _)
    }
  }

  def unpartitioned2 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = unpartitionedDelta(tmpDir)
    for {
      _ <- buildLakeWriter(config, TestAlerts.ignore).use(_.createTable *> clusterByEventName(config.output.good))
      result <- startUp(config)
    } yield result must beLeft
  }

  def unpartitioned8 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = unpartitionedDelta(tmpDir)
    for {
      before <- buildLakeWriter(config, TestAlerts.ignore).use { _ =>
                  IO.blocking(createUnclustered(SparkSession.active, config.output.good)) *> readClusteringDomain(config)
                }
      result <- startUp(config)
      after <- buildLakeWriter(config, TestAlerts.ignore).use(_ => readClusteringDomain(config))
    } yield (before must beNone) and (result must beLeft) and (after must beNone)
  }

  def unpartitioned9 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = unpartitionedDelta(tmpDir)
    for {
      before <- buildLakeWriter(config, TestAlerts.ignore).use { lakeWriter =>
                  lakeWriter.createTable *> clusterByNone(config.output.good) *> readClusteringDomain(config)
                }
      result <- startUp(config)
      after <- buildLakeWriter(config, TestAlerts.ignore).use(_ => readClusteringDomain(config))
    } yield (before must beSome(EmptyClustering)) and (result must beLeft) and (after must beSome(EmptyClustering))
  }

  def unpartitioned3 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config   = unpartitionedDelta(tmpDir)
    val entity   = StructField("unstruct_event_com_acme_thing_1", StringType, nullable = true)
    val schema   = StructType(SparkSchema.atomic :+ entity)
    val rows     = NonEmptyList.of(mkRow(schema, Some("app_a")))
    val viewName = "test_unpartitioned_entity"
    for {
      columns <- buildLakeWriter(config, TestAlerts.ignore).use { lakeWriter =>
                   lakeWriter.createTable *>
                     lakeWriter.initializeLocalDataFrame(viewName) *>
                     lakeWriter.localAppendRows(viewName, rows, schema) *>
                     lakeWriter.prepareCommit(viewName, 1, Map(Some("page_view") -> 1), Set(entity.name)) *>
                     lakeWriter.commit(viewName) *>
                     clusterByEventName(config.output.good) *>
                     IO.blocking(readTable(SparkSession.active, config.output.good).columns.toList)
                 }
      result <- startUp(config)
    } yield (result must beLeft) and (columns must contain(entity.name))
  }

  /**
   * Databricks' `REPLACE PARTITIONED BY WITH CLUSTER BY` leaves `load_tstamp_date` in place, and
   * OSS Delta has no such command, so the table is created in that shape directly.
   */
  def unpartitioned4 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config   = unpartitionedDelta(tmpDir)
    val schema   = StructType(SparkSchema.atomic)
    val rows     = NonEmptyList.of(mkRow(schema, Some("app_a")), mkRow(schema, None))
    val viewName = "test_unpartitioned_converted"
    buildLakeWriter(config, TestAlerts.ignore).use { lakeWriter =>
      for {
        spark <- IO.delay(SparkSession.active)
        _ <- IO.blocking(createConvertedShape(spark, config.output.good))
        _ <- lakeWriter.createTable
        _ <- lakeWriter.initializeLocalDataFrame(viewName)
        _ <- lakeWriter.localAppendRows(viewName, rows, schema)
        _ <- lakeWriter.prepareCommit(viewName, 2, Map(Some("page_view") -> 2), Set.empty)
        _ <- lakeWriter.commit(viewName)
        dates <- IO.blocking {
                   readTable(spark, config.output.good)
                     .select(col("load_tstamp_date") === to_date(col("load_tstamp")))
                     .collect()
                     .toList
                     .map(_.getBoolean(0))
                 }
      } yield dates must beEqualTo(List(true, true))
    }
  }

  def unpartitioned5 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    startupMismatchAlert(tmpDir, TestConfig.defaults(TestConfig.Delta, tmpDir), unpartitionedDelta(tmpDir), unpartitionedMismatchMessage)
  }

  def unpartitioned6 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    startupMismatchAlert(tmpDir, unpartitionedDelta(tmpDir), TestConfig.defaults(TestConfig.Delta, tmpDir), eventNameMismatchMessage)
  }

  def unpartitioned7 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val viewName     = "test_unpartitioned_end_to_end"
    val config       = unpartitionedDelta(tmpDir)
    val target       = config.output.good
    val location     = new File(target.asInstanceOf[Config.Delta].location)
    val eventNames   = List.fill(300)("page_view") ++ List.fill(100)("link_click") ++ List.fill(50)("page_ping")
    val counts       = eventNames.groupBy(identity).map { case (name, names) => Option(name) -> names.size }
    val partitioning = Config.WriterPartitioning(splitsPerFairShare = 2, minEventsPerSplit = 1)
    val writer       = writerFor(target)
    val windowId     = Writer.WindowId(UUID.randomUUID, viewName)

    SparkUtils.session[IO](config.spark, writer, target, cores = 4).use { spark =>
      val schema = StructType(SparkSchema.atomic)
      val rows   = NonEmptyList.fromListUnsafe(eventNames.map(mkRowNamed(schema, _)))
      for {
        _ <- writer.prepareTable[IO](spark, UUID.randomUUID)
        _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
        encoded <- SparkUtils.encodeBatch[IO](spark, rows, schema)
        staged <- SparkUtils.stageBatch[IO](spark, encoded, schema, stageOffHeap = false)
        _ <- SparkUtils.appendStagedBatch[IO](spark, viewName, staged.df, schema, shouldRestoreNullability = false)
        prepared <- SparkUtils.prepareFinalDataFrame[IO](
                      spark,
                      viewName,
                      nonAtomicColumnNames = Set.empty,
                      writerParallelism    = 3,
                      writerPartitioning   = partitioning,
                      partitionColumn      = target.tablePartitionColumn,
                      numEvents            = eventNames.size,
                      partitionCounts      = Map.empty
                    )
        shuffled <- SparkUtils.materializeShuffle[IO](spark, prepared)
        _ <- SparkUtils.replaceView[IO](viewName, shuffled.df)
        toWrite <- SparkUtils.readFinalDataFrame[IO](spark, viewName)
        writePeak <- peakTaskExecutionMemoryDuring(spark)(writer.write[IO](toWrite, windowId, isRetry = false))
        rowsPerName <- IO.blocking(countBy(readTable(spark, target), "event_name"))
        snapshot <- IO.blocking(deltaSnapshot(spark, target))
        files <- IO.blocking(parquetFiles(location))
      } yield List(
        target.tablePartitionColumn must beNone,
        // No sort in the commit, which `packing` shows this measure detects
        writePeak must_== WritePeak(0L, files.size),
        rowsPerName must beEqualTo(counts.view.mapValues(_.toLong).toMap),
        snapshot.metadata.partitionColumns must beEmpty,
        clusteringDomain(snapshot) must beSome(contain("[[\"load_tstamp\"]]")),
        // No partition directories, and no more than one file per writer task
        files.map(_.getParentFile.getCanonicalPath).distinct must beEqualTo(List(location.getCanonicalPath)),
        files.size must beBetween(1, 3)
      ).reduce(_ and _)
    }
  }

  /**
   * The largest execution memory any task writing records claimed while `write` ran, which is
   * non-zero when the commit sorted, and how many such tasks there were. See
   * `SnowplowSparkBlockProbe.PeakTaskExecutionMemory`.
   */
  private def peakTaskExecutionMemoryDuring(spark: SparkSession)(write: IO[Unit]): IO[WritePeak] =
    Resource
      .make(IO.blocking(SnowplowSparkBlockProbe.trackPeakTaskExecutionMemory(spark)))(t => IO.blocking(t.stop()))
      .use(t => write *> IO.blocking(WritePeak(t.peak, t.tasksWritingRecords)))

  private def newTable(target: TestConfig.Target, expected: Set[String]) = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = withPartitionColumn(TestConfig.defaults(target, tmpDir), Some(Config.PartitionColumn.AppId))
    val writer = writerFor(config.output.good)
    SparkUtils.session[IO](config.spark, writer, config.output.good, cores = 2).use { spark =>
      for {
        _ <- writer.prepareTable[IO](spark, UUID.randomUUID)
        latest <- writer.readPartitioning[IO](spark, latest = true)
        loaded <- writer.readPartitioning[IO](spark, latest = false)
      } yield List(
        latest must beEqualTo(expected),
        loaded must beEqualTo(expected),
        writer.expectedPartitioning must beEqualTo(expected)
      ).reduce(_ and _)
    }
  }

  /**
   * Over the composition `LakeWriter.prepareCommit` and `commit` perform, as
   * `DeltaPartitioningSpec` is, so the plan the files are held against is the one the window was
   * packed with. One app_id is big enough to be split, so a writer that regrouped rows would be
   * caught; null is a key too.
   *
   * With `concurrentWriters`, `spark.sql.maxConcurrentOutputFileWriters` lets `FileFormatWriter`
   * keep a writer open per partition value instead of sorting, so the same write claims no
   * execution memory. That is the control for the sort measure, here and in `unpartitioned7`.
   */
  private def packing(target: TestConfig.Target, concurrentWriters: Boolean) = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val viewName      = s"test_app_id_packing_$target"
    val config        = withPartitionColumn(TestConfig.defaults(target, tmpDir), Some(Config.PartitionColumn.AppId))
    val writer        = writerFor(config.output.good)
    val appIds        = List.fill(500)(Some("app_hot")) ++ (1 to 200).toList.map(i => Some(s"app_cold_${i % 2}")) ++ List.fill(100)(None)
    val counts        = appIds.groupBy(identity).view.mapValues(_.size).toMap
    val partitioning  = Config.WriterPartitioning(splitsPerFairShare = 2, minEventsPerSplit = 1)
    val plan          = WriterPartitioner.plan(counts, writerParallelism = 3, partitioning)
    val expectedFiles = plan.assignments.map { case (appId, partitions) => appId -> partitions.distinct.size }.toMap

    SparkUtils.session[IO](config.spark, writer, config.output.good, cores = 4).use { spark =>
      val schema = StructType(SparkSchema.atomic)
      val rows   = NonEmptyList.fromListUnsafe(appIds.map(mkRow(schema, _)))

      for {
        _ <- writer.prepareTable[IO](spark, UUID.randomUUID)
        _ <- IO.blocking(if (concurrentWriters) spark.conf.set("spark.sql.maxConcurrentOutputFileWriters", "100"))
        _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
        encoded <- SparkUtils.encodeBatch[IO](spark, rows, schema)
        staged <- SparkUtils.stageBatch[IO](spark, encoded, schema, stageOffHeap = false)
        _ <- SparkUtils.appendStagedBatch[IO](spark, viewName, staged.df, schema, shouldRestoreNullability = false)
        prepared <- SparkUtils.prepareFinalDataFrame[IO](
                      spark,
                      viewName,
                      nonAtomicColumnNames = Set.empty,
                      writerParallelism    = 3,
                      writerPartitioning   = partitioning,
                      partitionColumn      = Some(Config.PartitionColumn.AppId),
                      numEvents            = counts.values.sum,
                      partitionCounts      = counts
                    )
        shuffled <- SparkUtils.materializeShuffle[IO](spark, prepared)
        _ <- SparkUtils.replaceView[IO](viewName, shuffled.df)
        toWrite <- SparkUtils.readFinalDataFrame[IO](spark, viewName)
        writePeak <-
          peakTaskExecutionMemoryDuring(spark)(writer.write[IO](toWrite, Writer.WindowId(UUID.randomUUID, viewName), isRetry = false))
        rowsPerAppId <- IO.blocking(countBy(readTable(spark, config.output.good), "app_id"))
        filesPerAppId <- IO.blocking(dataFilesPerAppId(spark, config.output.good))
      } yield List(
        // The split app_id is what a plain re-cluster could not also satisfy.
        expectedFiles.values.max must beGreaterThan(1),
        // The commit sorted by the partition columns, as `Writer.write` describes, unless the
        // concurrent writers replaced the sort
        if (concurrentWriters) writePeak.peak must_== 0L else writePeak.peak must beGreaterThan(0L),
        writePeak.tasks must beGreaterThan(0),
        rowsPerAppId must beEqualTo(counts.view.mapValues(_.toLong).toMap),
        filesPerAppId must beEqualTo(expectedFiles)
      ).reduce(_ and _)
    }
  }

  /**
   * A table created with the default partitioning, then a loader restarted with `app_id`. The
   * restart's `createTable` must alert with the setup message rather than retrying as transient,
   * and it never stops retrying a setup error, so the example stops at the first alert. The table
   * must have no commit beyond the one its creation made, such as Iceberg's empty startup commit.
   */
  private def startupMismatch(target: TestConfig.Target) = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val asCreated = TestConfig.defaults(target, tmpDir)
    startupMismatchAlert(tmpDir, asCreated, withPartitionColumn(asCreated, Some(Config.PartitionColumn.AppId)), mismatchMessage)
  }

  private def startupMismatchAlert(
    tmpDir: Path,
    asCreated: AnyConfig,
    restarted: AnyConfig,
    message: String
  ) =
    for {
      _ <- buildLakeWriter(asCreated, TestAlerts.ignore).use(_.createTable)
      commitsBefore <- commitsInTable(tmpDir, asCreated.output.good)
      alerts <- Deferred[IO, Alert]
      result <- buildLakeWriter(restarted, TestAlerts.capture(alerts)).use { lakeWriter =>
                  IO.race(lakeWriter.createTable, alerts.get)
                }
      commitsAfter <- commitsInTable(tmpDir, asCreated.output.good)
    } yield List(
      result must beLike { case Right(Alert.FailedToCreateEventsTable(causes)) =>
        causes.show must contain(message)
      },
      commitsAfter must beEqualTo(commitsBefore)
    ).reduce(_ and _)

  /**
   * The table is changed under a running loader, which then commits up to two windows. A commit
   * must raise the mismatch itself, rather than an alert from inside `RemoteTable.commit`'s
   * retries, and leave its window unwritten. The check reads what the loader's previous commit left
   * behind, so a change it did not make itself may let the first window through; the second must
   * not be.
   *
   * Iceberg's change bypasses the `CachingCatalog` table the loader holds, so the second window
   * fails only if the check reads the table as the first window's commit refreshed it.
   */
  private def midRunMismatch(target: TestConfig.Target) = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = withPartitionColumn(TestConfig.defaults(target, tmpDir), Some(Config.PartitionColumn.AppId))
    val schema = StructType(SparkSchema.atomic)
    val rows   = NonEmptyList.of(mkRow(schema, Some("app_a")), mkRow(schema, None))
    val counts = Map(Some("app_a") -> 1, None -> 1)

    buildLakeWriter(config, TestAlerts.ignore).use { lakeWriter =>
      def commitWindow(viewName: String): IO[Either[Throwable, Unit]] =
        lakeWriter.initializeLocalDataFrame(viewName) *>
          lakeWriter.localAppendRows(viewName, rows, schema) *>
          lakeWriter.prepareCommit(viewName, rows.size, counts, Set.empty) *>
          lakeWriter.commit(viewName).attempt

      for {
        _ <- lakeWriter.createTable
        spark <- IO.delay(SparkSession.active)
        _ <- IO.blocking(repartitionByEventName(spark, tmpDir, config.output.good))
        first <- commitWindow(s"test_mid_run_mismatch_${target}_1")
        failed <- first match {
                    case Left(e)  => IO.pure(Left(e))
                    case Right(_) => commitWindow(s"test_mid_run_mismatch_${target}_2")
                  }
        written <- IO.blocking(readTable(spark, config.output.good).count())
      } yield List(
        failed must beLeft(beAnInstanceOf[Writer.PartitioningMismatchException]),
        written must beEqualTo(if (first.isRight) rows.size.toLong else 0L)
      ).reduce(_ and _)
    }
  }
}

object PartitionColumnSpec {

  private final case class WritePeak(peak: Long, tasks: Int)

  private val mismatchMessage =
    "The table's partitioning does not match the loader's configuration. The loader is configured to load into a table partitioned by the date of load_tstamp and app_id"

  private val eventNameMismatchMessage =
    "The table's partitioning does not match the loader's configuration. The loader is configured to load into a table partitioned by the date of load_tstamp and event_name"

  private val unpartitionedMismatchMessage =
    "The table's partitioning does not match the loader's configuration. The loader is configured to load into a table that is not partitioned"

  private val icebergTable = "iceberg_catalog.`test`.`events`"

  /** `None` is for Delta only, as the config decoder enforces */
  def withPartitionColumn(config: AnyConfig, partitionColumn: Option[Config.PartitionColumn]): AnyConfig =
    config.copy(output = config.output.copy(good = (config.output.good, partitionColumn) match {
      case (d: Config.Delta, _)         => d.copy(partitionColumn = partitionColumn)
      case (i: Config.Iceberg, Some(c)) => i.copy(partitionColumn = c)
      case (_: Config.Iceberg, None)    => throw new IllegalArgumentException("The loader always partitions an Iceberg table")
    }))

  private def unpartitionedDelta(tmpDir: Path): AnyConfig =
    withPartitionColumn(TestConfig.defaults(TestConfig.Delta, tmpDir), None)

  private def deltaSnapshot(spark: SparkSession, target: Config.Target): Snapshot =
    DeltaLog.forTable(spark, target.asInstanceOf[Config.Delta].location.toString).update()

  private def statsColumns(snapshot: Snapshot): List[String] =
    snapshot.metadata.configuration.get("delta.dataSkippingStatsColumns").toList.flatMap(_.split(",").toList)

  /**
   * The configuration of the `delta.clustering` domain, a JSON object holding the physical column
   * names
   */
  private def clusteringDomain(snapshot: Snapshot): Option[String] =
    snapshot.domainMetadata.find(_.domain === "delta.clustering").map(_.configuration)

  /** The `delta.clustering` domain's configuration once `CLUSTER BY NONE` has removed every key */
  private val EmptyClustering = "{\"clusteringColumns\":[],\"domainName\":\"delta.clustering\"}"

  /**
   * Reads the `delta.clustering` domain, inside a `buildLakeWriter` so that a Spark session is up
   */
  private def readClusteringDomain(config: AnyConfig): IO[Option[String]] =
    IO.blocking(clusteringDomain(deltaSnapshot(SparkSession.active, config.output.good)))

  /** Changes the clustering keys, as Databricks can. The table's schema is unchanged. */
  private def clusterByEventName(target: Config.Target): IO[Unit] =
    IO.blocking {
      SparkSession.active.sql(s"ALTER TABLE delta.`${target.asInstanceOf[Config.Delta].location}` CLUSTER BY (load_tstamp, event_name)")
    }.void

  private def clusterByNone(target: Config.Target): IO[Unit] =
    IO.blocking {
      SparkSession.active.sql(s"ALTER TABLE delta.`${target.asInstanceOf[Config.Delta].location}` CLUSTER BY NONE")
    }.void

  /**
   * Runs the loader's `createTable` until it completes, `Left`, or raises its first alert, `Right`
   */
  private def startUp(config: AnyConfig): IO[Either[Unit, Alert]] =
    Deferred[IO, Alert].flatMap { alerts =>
      buildLakeWriter(config, TestAlerts.capture(alerts)).use { lakeWriter =>
        IO.race(lakeWriter.createTable, alerts.get)
      }
    }

  /**
   * An unpartitioned table, clustered by `load_tstamp`, that keeps the generated `load_tstamp_date`
   */
  private def createConvertedShape(spark: SparkSession, target: Config.Target): Unit = {
    val builder = DeltaTable
      .create(spark)
      .location(target.asInstanceOf[Config.Delta].location.toString)
      .tableName("events_converted") // Not the loader's name, so its createIfNotExists still reaches the table
      .clusterBy("load_tstamp")
    // The loader's stats list, which covers load_tstamp; Delta's default of the first 32 columns does not
    target.asInstanceOf[Config.Delta].deltaTableProperties.foreach { case (k, v) => builder.property(k, v) }
    AtomicFields.withLoadTstamp.foreach(f => builder.addColumn(SparkSchema.asSparkField(f, true)))
    builder.addColumn {
      DeltaTable
        .columnBuilder(spark, "load_tstamp_date")
        .dataType("DATE")
        .generatedAlwaysAs("CAST(load_tstamp AS DATE)")
        .nullable(false)
        .build()
    }: Unit
    builder.execute(): Unit
  }

  /** The loader's columns and table properties, with neither partitioning nor clustering. */
  private def createUnclustered(spark: SparkSession, target: Config.Target): Unit = {
    val builder = DeltaTable
      .create(spark)
      .location(target.asInstanceOf[Config.Delta].location.toString)
      .tableName("events_unclustered") // Not the loader's name, so its createIfNotExists still reaches the table
    target.asInstanceOf[Config.Delta].deltaTableProperties.foreach { case (k, v) => builder.property(k, v) }
    AtomicFields.withLoadTstamp.foreach(f => builder.addColumn(SparkSchema.asSparkField(f, true)))
    builder.execute(): Unit
  }

  private def writerFor(target: Config.Target): Writer =
    target match {
      case d: Config.Delta   => new DeltaWriter(d)
      case i: Config.Iceberg => new IcebergWriter(i)
    }

  private def readTable(spark: SparkSession, target: Config.Target): DataFrame =
    target match {
      case d: Config.Delta   => spark.read.format("delta").load(d.location.toString)
      case _: Config.Iceberg => spark.table(icebergTable)
    }

  private def countBy(df: DataFrame, column: String): Map[Option[String], Long] =
    df.groupBy(column).count().collect().toList.map(r => Option(r.getString(0)) -> r.getLong(1)).toMap

  /** Delta writes a directory per partition value; Iceberg's object storage layout does not. */
  private def dataFilesPerAppId(spark: SparkSession, target: Config.Target): Map[Option[String], Int] =
    target match {
      case d: Config.Delta =>
        parquetFiles(new File(d.location))
          .groupBy(_.getParentFile.getName.stripPrefix("app_id=") match {
            case "__HIVE_DEFAULT_PARTITION__" => None
            case appId                        => Some(appId)
          })
          .view
          .mapValues(_.size)
          .toMap
      case _: Config.Iceberg =>
        countBy(spark.table(s"$icebergTable.files").select("partition.app_id"), "app_id").view.mapValues(_.toInt).toMap
    }

  /**
   * Changes the table's partitioning from `app_id` to `event_name`. Delta's goes through the
   * session, as an empty overwrite, so the table still holds no rows. Iceberg's goes through a
   * catalog of its own, as another process's would.
   */
  private def repartitionByEventName(
    spark: SparkSession,
    tmpDir: Path,
    target: Config.Target
  ): Unit =
    target match {
      case d: Config.Delta =>
        // Delta refuses to partition by a column it collects stats on
        spark.sql(s"ALTER TABLE delta.`${d.location}` SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'load_tstamp')"): Unit
        readTable(spark, target)
          .limit(0)
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .partitionBy("load_tstamp_date", "event_name")
          .save(d.location.toString)
      case i: Config.Iceberg =>
        withOwnCatalog(tmpDir, i)(_.updateSpec().removeField("app_id").addField("event_name").commit())
    }

  /** Iceberg's snapshots, or Delta's versions */
  private def commitsInTable(tmpDir: Path, target: Config.Target): IO[Int] =
    IO.blocking {
      target match {
        case d: Config.Delta =>
          Option(new File(new File(d.location), "_delta_log").listFiles()).toList.flatten.count(_.getName.matches("""\d+\.json"""))
        case i: Config.Iceberg =>
          withOwnCatalog(tmpDir, i)(_.snapshots.asScala.size)
      }
    }

  private def withOwnCatalog[A](tmpDir: Path, iceberg: Config.Iceberg)(f: Table => A): A = {
    val catalog = new HadoopCatalog(new Configuration, tmpDir.toNioPath.toString)
    try f(catalog.loadTable(TableIdentifier.of(iceberg.database, iceberg.table)))
    finally catalog.close()
  }

  private def buildLakeWriter(config: AnyConfig, appHealth: AppHealth.Interface[IO, Alert, RuntimeService]): Resource[IO, LakeWriter[IO]] =
    LakeWriter.build[IO](
      config.spark,
      config.output.good,
      respectIgluNullability = true,
      cores                  = 4,
      appHealth,
      Config.Retries(Retrying.Config.ForSetup(30.seconds), Retrying.Config.ForTransient(1.second, 5)),
      DestinationSetupError.check.orElse(TableFormatSetupError.check(config.output.good))
    )

  private object TestAlerts {
    def capture(alerts: Deferred[IO, Alert]): AppHealth.Interface[IO, Alert, RuntimeService] =
      new AppHealth.Interface[IO, Alert, RuntimeService] {
        def beHealthyForSetup: IO[Unit]                                     = IO.unit
        def beUnhealthyForSetup(alert: Alert): IO[Unit]                     = alerts.complete(alert).void
        def beHealthyForRuntimeService(service: RuntimeService): IO[Unit]   = IO.unit
        def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] = IO.unit
      }

    val ignore: AppHealth.Interface[IO, Alert, RuntimeService] =
      new AppHealth.Interface[IO, Alert, RuntimeService] {
        def beHealthyForSetup: IO[Unit]                                     = IO.unit
        def beUnhealthyForSetup(alert: Alert): IO[Unit]                     = IO.unit
        def beHealthyForRuntimeService(service: RuntimeService): IO[Unit]   = IO.unit
        def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] = IO.unit
      }
  }

  private def mkRow(schema: StructType, appId: Option[String]): Row =
    mkRow(schema, appId, "page_view")

  private def mkRowNamed(schema: StructType, eventName: String): Row =
    mkRow(schema, None, eventName)

  private def mkRow(
    schema: StructType,
    appId: Option[String],
    eventName: String
  ): Row =
    Row.fromSeq(schema.fields.toSeq.map {
      case f if f.name === "app_id"     => appId.orNull
      case f if f.name === "event_name" => eventName
      case f if f.nullable              => null
      case f                            => placeholder(f.dataType)
    })

  private def placeholder(dataType: DataType): Any = dataType match {
    case StringType    => UUID.randomUUID.toString
    case TimestampType => new Timestamp(0L)
    case IntegerType   => 0
    case LongType      => 0L
    case DoubleType    => 0.0d
    case BooleanType   => false
    case other         => throw new IllegalStateException(s"No placeholder for a required field of type $other")
  }

  private def parquetFiles(root: File): List[File] =
    if (root.isDirectory) Option(root.listFiles()).toList.flatten.filterNot(_.getName === "_delta_log").flatMap(parquetFiles)
    else List(root).filter(_.getName.endsWith(".parquet"))
}
