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
import cats.implicits._
import cats.effect.{IO, Resource}
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

import fs2.io.file.Files
import org.apache.spark.sql.SnowplowSparkBlockProbe

import com.snowplowanalytics.snowplow.runtime.Retrying
import com.snowplowanalytics.snowplow.lakes._

import scala.concurrent.duration.DurationLong

/**
 * Drives a real Spark session. The retry, alert and health behaviour of the operations that reach
 * the lake is in `RemoteTableSpec`, which needs no Spark.
 */
class LakeWriterSpec extends Specification with CatsEffect {
  import LakeWriterSpec._

  // Sequential: e1 and e2 create shuffles in the shared Spark session and e1 asserts on a diff of
  // the global MapOutputTracker, and specs2 runs examples concurrently unless told not to.
  def is = sequential ^ s2"""
  The lake writer should:
    release the window's shuffle when the window is dropped, not leave it to the ContextCleaner $e1
    account for the window's shuffle bytes and staged blocks across the window's lifecycle $e2
    not let a window that staged nothing overwrite the storage peak of one that did $e3
    account for the staged blocks of a window that had no shuffle to hand over to $e4
    sum the shuffle bytes of every open window, and keep one when another is dropped $e5
  """

  // e19 in SparkUtilsSpec proves dropView releases a shuffle id it is handed. This proves the wiring
  // either side of that: that prepareCommit records the id it created, and that
  // removeDataFrameFromDisk hands that same id over. Nothing else covers it, and the cost of it
  // breaking is a window's worth of shuffle files per window left on spark.local.dir, which no test
  // and no metric would show.
  //
  // Deliberately never calls commit. The table writer creates a shuffle of its own during the
  // write, which the loader neither tracks nor is responsible for, so a tracker diff taken across a
  // full commit could not tell that one from a leak of ours. Stopping short of the write leaves
  // prepareCommit's shuffle as the only one in flight.
  def e1 = Files[IO].tempDirectory.use { tmpDir =>
    val viewName = "test_shuffle_released_on_drop_e1"
    val schema   = eventSchema
    val rows     = NonEmptyList.of(atomicRow("e1", "page_view"), atomicRow("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    buildLakeWriter(config, cores = 4).use { lakeWriter =>
      for {
        before <- IO.blocking(SnowplowSparkBlockProbe.registeredShuffleIds)
        _ <- lakeWriter.initializeLocalDataFrame(viewName)
        _ <- lakeWriter.localAppendRows(viewName, rows, schema)
        _ <- lakeWriter.prepareCommit(viewName, eventNameCounts.values.sum, eventNameCounts, Set.empty)
        created <- IO.blocking(SnowplowSparkBlockProbe.registeredShuffleIds -- before)
        _ <- lakeWriter.removeDataFrameFromDisk(viewName)
        leaked <- IO.blocking(created.filter(SnowplowSparkBlockProbe.shuffleRegistered))
      } yield (created.nonEmpty, leaked) must beEqualTo((true, Set.empty[Int]))
    }
  }

  // The gauges are only as good as this accounting, and none of it is visible from Processing: the
  // shuffle figure has to reach zero when the window is dropped or it accumulates for the life of
  // the app, and the block manager measurement has to be taken by prepareCommit rather than left
  // unset. Both are read through the real LakeWriter, over the lifecycle Processing performs.
  def e2 = Files[IO].tempDirectory.use { tmpDir =>
    val viewName = "test_metric_accounting_e2"
    val schema   = eventSchema
    val rows     = NonEmptyList.of(atomicRow("e1", "page_view"), atomicRow("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    buildLakeWriter(config, cores = 4).use { lakeWriter =>
      for {
        _ <- lakeWriter.initializeLocalDataFrame(viewName)
        _ <- lakeWriter.localAppendRows(viewName, rows, schema)
        shuffleBytesBefore <- lakeWriter.getShuffleDiskBytes
        peakBefore <- lakeWriter.getStorageUsage(viewName)
        _ <- lakeWriter.prepareCommit(viewName, eventNameCounts.values.sum, eventNameCounts, Set.empty)
        shuffleBytesDuring <- lakeWriter.getShuffleDiskBytes
        // Recorded by prepareCommit only where it released the batches, so only where it shuffled.
        peakAfterPrepare <- lakeWriter.getStorageUsage(viewName)
        _ <- lakeWriter.recordBlockManagerPeak(viewName)
        // Recorded either way by the time the window is dropped, which is what Processing relies on.
        peakAtDrop <- lakeWriter.getStorageUsage(viewName)
        _ <- lakeWriter.removeDataFrameFromDisk(viewName)
        shuffleBytesAfter <- lakeWriter.getShuffleDiskBytes
      } yield (
        shuffleBytesBefore,
        shuffleBytesDuring > 0L,
        shuffleBytesAfter,
        peakBefore.isDefined,
        peakAfterPrepare.isDefined,
        peakAtDrop.isDefined
      ) must beEqualTo((0L, true, 0L, false, true, true))
    }
  }

  // recordBlockManagerPeak keys on whether the window still holds its batches, not on whether it
  // lacks a shuffle - different tests for a window that staged nothing at all. A window with
  // nothing staged has no peak of its own, and must not acquire one: reporting a measurement taken
  // while it was open would describe whatever the *next* window had just started accumulating.
  def e3 = Files[IO].tempDirectory.use { tmpDir =>
    val busyView  = "test_peak_survives_empty_window_busy_e3"
    val emptyView = "test_peak_survives_empty_window_empty_e3"
    val schema    = eventSchema
    val rows      = NonEmptyList.of(atomicRow("e1", "page_view"), atomicRow("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    buildLakeWriter(config, cores = 4).use { lakeWriter =>
      for {
        _ <- lakeWriter.initializeLocalDataFrame(busyView)
        _ <- lakeWriter.localAppendRows(busyView, rows, schema)
        _ <- lakeWriter.prepareCommit(busyView, eventNameCounts.values.sum, eventNameCounts, Set.empty)
        _ <- lakeWriter.recordBlockManagerPeak(busyView)
        busyPeak <- lakeWriter.getStorageUsage(busyView)
        // A window that never staged anything, so it has no peak of its own to report.
        _ <- lakeWriter.initializeLocalDataFrame(emptyView)
        _ <- lakeWriter.recordBlockManagerPeak(emptyView)
        emptyPeak <- lakeWriter.getStorageUsage(emptyView)
        busyPeakAfter <- lakeWriter.getStorageUsage(busyView)
        _ <- lakeWriter.removeDataFrameFromDisk(busyView)
        _ <- lakeWriter.removeDataFrameFromDisk(emptyView)
      } yield (busyPeak.isDefined, emptyPeak, busyPeakAfter) must beEqualTo((true, None, busyPeak))
    }
  }

  // Two cores gives writerParallelism 1, so prepareFinalDataFrame coalesces and there is no
  // exchange - the branch most deployments take. prepareCommit then releases nothing and records no
  // peak; recordBlockManagerPeak at the drop is what covers it.
  def e4 = Files[IO].tempDirectory.use { tmpDir =>
    val viewName = "test_no_shuffle_accounting_e4"
    val schema   = eventSchema
    val rows     = NonEmptyList.of(atomicRow("e1", "page_view"), atomicRow("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    buildLakeWriter(config, cores = 2).use { lakeWriter =>
      for {
        _ <- lakeWriter.initializeLocalDataFrame(viewName)
        _ <- lakeWriter.localAppendRows(viewName, rows, schema)
        _ <- lakeWriter.prepareCommit(viewName, eventNameCounts.values.sum, eventNameCounts, Set.empty)
        shuffleBytes <- lakeWriter.getShuffleDiskBytes
        peakAfterPrepare <- lakeWriter.getStorageUsage(viewName)
        _ <- lakeWriter.recordBlockManagerPeak(viewName)
        peakAtDrop <- lakeWriter.getStorageUsage(viewName)
        _ <- lakeWriter.removeDataFrameFromDisk(viewName)
      } yield (shuffleBytes, peakAfterPrepare.isDefined, peakAtDrop.isDefined) must beEqualTo((0L, false, true))
    }
  }

  // The scaladoc makes summing over open windows the whole point of this gauge: above one window's
  // worth is the "windows overlap" invariant breaking. A regression to reading only the ending
  // window's shuffle would pass every other example here and turn the detector into a constant.
  def e5 = Files[IO].tempDirectory.use { tmpDir =>
    val firstView  = "test_two_live_shuffles_first_e5"
    val secondView = "test_two_live_shuffles_second_e5"
    val schema     = eventSchema
    val rows       = NonEmptyList.of(atomicRow("e1", "page_view"), atomicRow("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    buildLakeWriter(config, cores = 4).use { lakeWriter =>
      for {
        _ <- lakeWriter.initializeLocalDataFrame(firstView)
        _ <- lakeWriter.localAppendRows(firstView, rows, schema)
        _ <- lakeWriter.prepareCommit(firstView, eventNameCounts.values.sum, eventNameCounts, Set.empty)
        oneWindow <- lakeWriter.getShuffleDiskBytes
        // A second window reaching prepareCommit before the first is dropped, which is what an
        // overrunning commit produces.
        _ <- lakeWriter.initializeLocalDataFrame(secondView)
        _ <- lakeWriter.localAppendRows(secondView, rows, schema)
        _ <- lakeWriter.prepareCommit(secondView, eventNameCounts.values.sum, eventNameCounts, Set.empty)
        twoWindows <- lakeWriter.getShuffleDiskBytes
        _ <- lakeWriter.removeDataFrameFromDisk(firstView)
        afterFirstDropped <- lakeWriter.getShuffleDiskBytes
        _ <- lakeWriter.removeDataFrameFromDisk(secondView)
        afterBothDropped <- lakeWriter.getShuffleDiskBytes
      } yield (
        oneWindow > 0L,
        twoWindows > oneWindow,
        afterFirstDropped > 0L,
        afterFirstDropped < twoWindows,
        afterBothDropped
      ) must beEqualTo((true, true, true, true, 0L))
    }
  }

}

object LakeWriterSpec {

  // prepareCommit selects the atomic columns and repartitions by event_name and event_id. Only those
  // two are set, so the others are nullable here.
  private val eventSchema = StructType(SparkSchema.atomic.map(f => f.copy(nullable = f.name =!= "event_id")))

  private def atomicRow(eventId: String, eventName: String): Row =
    Row.fromSeq(eventSchema.fields.toSeq.map { f =>
      f.name match {
        case "event_id"   => eventId
        case "event_name" => eventName
        case _            => null
      }
    })

  /**
   * The histogram of the two-row batch every example below appends, so `prepareCommit` plans a
   * partitioning over the same keys its rows carry rather than over an empty one.
   */
  val eventNameCounts: Map[Option[String], Int] = Map(Some("page_view") -> 1, Some("page_ping") -> 1)

  private val retriesConfig = Config.Retries(
    Retrying.Config.ForSetup(30.seconds),
    Retrying.Config.ForTransient(1.second, 5)
  )

  private def buildLakeWriter(config: AnyConfig, cores: Int): Resource[IO, LakeWriter[IO]] =
    LakeWriter.build[IO](
      config.spark,
      config.output.good,
      respectIgluNullability = true,
      cores                  = cores,
      TestSparkEnvironment.dummyAppHealth,
      retriesConfig,
      PartialFunction.empty
    )
}
