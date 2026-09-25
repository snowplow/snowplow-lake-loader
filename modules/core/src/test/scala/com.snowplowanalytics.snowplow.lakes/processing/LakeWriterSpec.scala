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
import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

import fs2.io.file.Files
import org.apache.spark.sql.SnowplowSparkBlockProbe

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.lakes._

import scala.concurrent.duration.DurationLong

/**
 * Every example using the mock writer below runs under `TestControl.executeEmbed`, including those
 * whose mocks never fail.
 *
 * `Retrying`'s transient policy is `RetryPolicies.fullJitter`, whose delay is random and doubles
 * per attempt, so an example reaching a backoff sleeps for real and can overrun `CatsEffect`'s
 * `Timeout`. `cats-effect-testing` reports a timeout as a skip rather than a failure, so such an
 * example asserts nothing and still leaves the suite green.
 *
 * An example whose mocks all succeed never reaches a backoff, so it passes without `executeEmbed`
 * until one of its mocks starts failing - hence on all of them rather than only the failing ones.
 * The examples driving a real Spark session call nothing that retries, and do not use it.
 */
class LakeWriterSpec extends Specification with CatsEffect {
  import LakeWriterSpec._

  // Sequential: e9 and e10 create shuffles in the shared Spark session and e9 asserts on a diff of
  // the global MapOutputTracker, and specs2 runs examples concurrently unless told not to.
  def is = sequential ^ s2"""
  The lake writer should:
    become healthy after creating the table $e1
    retry creating table and send alerts when there is a setup exception $e2
    retry creating table if there is a transient exception, with limited number of attempts and no monitoring alerts $e3
    become healthy after recovering from an earlier setup error $e4
    become healthy after recovering from an earlier transient error $e5
    become healthy after committing to the lake $e6
    become unhealthy after failure to commit to the lake $e7
    let a failed commit preparation through without retrying, alerting or touching setup health $e8
    release the window's shuffle when the window is dropped, not leave it to the ContextCleaner $e9
    account for the window's shuffle bytes and staged blocks across the window's lifecycle $e10
    not let a window that staged nothing overwrite the storage peak of one that did $e11
    account for the staged blocks of a window that had no shuffle to hand over to $e12
    sum the shuffle bytes of every open window, and keep one when another is dropped $e13
  """

  def e1 =
    control().flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      val test = for {
        _ <- wrappedLakeWriter.createTable
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }

  def e2 = {
    val mocks = Mocks(List.fill(100)(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.SentAlert(0L),
        Action.CreateTableAttempted,
        Action.SentAlert(30L),
        Action.CreateTableAttempted,
        Action.SentAlert(90L),
        Action.CreateTableAttempted,
        Action.SentAlert(210L)
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        _ => "this is a setup error"
      )

      val test = for {
        fiber <- wrappedLakeWriter.createTable.voidError.start
        _ <- IO.sleep(4.minutes)
        _ <- fiber.cancel
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e3 = {
    val mocks = Mocks(List.fill(100)(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter)
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      val test = for {
        _ <- wrappedLakeWriter.createTable.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e4 = {
    val mocks = Mocks(List(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.SentAlert(0L),
        Action.CreateTableAttempted,
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        _ => "this is a setup error"
      )

      val test = for {
        _ <- wrappedLakeWriter.createTable.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e5 = {
    val mocks = Mocks(List(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      val test = for {
        _ <- wrappedLakeWriter.createTable.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e6 =
    control().flatMap { c =>
      val expected = Vector(
        Action.CommitAttempted("testview"),
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      val test = for {
        _ <- wrappedLakeWriter.commit("testview")
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }

  def e7 = {
    val numAttempts = retriesConfig.transientErrors.attempts
    val mocks = Mocks(
      List(Response.Success) ++ List.fill(numAttempts)(Response.ExceptionThrown(new RuntimeException("boom!")))
    )

    control(mocks).flatMap { c =>
      val commitAttempts = (1 to numAttempts).flatMap { _ =>
        Vector(
          Action.CommitAttempted("testview2"),
          Action.BecameUnhealthy(RuntimeService.SparkWriter)
        )
      }

      val expected = Vector(
        Action.CommitAttempted("testview1"),
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      ) ++ commitAttempts

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      val test = for {
        _ <- wrappedLakeWriter.commit("testview1")
        _ <- wrappedLakeWriter.commit("testview2").voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  // prepareCommit is deliberately not wrapped in Retrying.withRetries, unlike commit. It is local
  // work, so nothing it raises can be triaged as a customer setup error, and its success is no
  // evidence that the destination is reachable. This pins that: if the wrapper is ever re-added,
  // the extra attempts and the health transitions both show up here.
  def e8 = {
    val boom = new RuntimeException("boom!")

    control(Mocks(List(Response.ExceptionThrown(boom)))).flatMap { c =>
      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      val test = for {
        result <- wrappedLakeWriter.prepareCommit("testview", Map.empty).attempt
        state <- c.state.get
      } yield (result.left.toOption, state) must beEqualTo((Some(boom), Vector(Action.PrepareCommitAttempted("testview"))))

      // Embedded like the rest, even though the unwrapped path it asserts on never sleeps: if the
      // wrapper this example exists to catch were re-added, the backoff would put it over the
      // timeout and it would be reported as a skip rather than the failure it should be.
      TestControl.executeEmbed(test)
    }
  }

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
  def e9 = Files[IO].tempDirectory.use { tmpDir =>
    val viewName = "test_shuffle_released_on_drop_e9"
    val schema = StructType(
      Array(
        StructField("event_id", StringType, nullable   = false),
        StructField("event_name", StringType, nullable = false)
      )
    )
    val rows = NonEmptyList.of(Row("e1", "page_view"), Row("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    LakeWriter.build[IO](config.spark, config.output.good, respectIgluNullability = true, cores = 4).use { lakeWriter =>
      for {
        before <- IO.blocking(SnowplowSparkBlockProbe.registeredShuffleIds)
        _ <- lakeWriter.initializeLocalDataFrame(viewName)
        _ <- lakeWriter.localAppendRows(viewName, rows, schema)
        _ <- lakeWriter.prepareCommit(viewName, eventNameCounts)
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
  def e10 = Files[IO].tempDirectory.use { tmpDir =>
    val viewName = "test_metric_accounting_e10"
    val schema = StructType(
      Array(
        StructField("event_id", StringType, nullable   = false),
        StructField("event_name", StringType, nullable = false)
      )
    )
    val rows = NonEmptyList.of(Row("e1", "page_view"), Row("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    LakeWriter.build[IO](config.spark, config.output.good, respectIgluNullability = true, cores = 4).use { lakeWriter =>
      for {
        _ <- lakeWriter.initializeLocalDataFrame(viewName)
        _ <- lakeWriter.localAppendRows(viewName, rows, schema)
        shuffleBytesBefore <- lakeWriter.getShuffleDiskBytes
        peakBefore <- lakeWriter.getStorageUsage(viewName)
        _ <- lakeWriter.prepareCommit(viewName, eventNameCounts)
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
  def e11 = Files[IO].tempDirectory.use { tmpDir =>
    val busyView  = "test_peak_survives_empty_window_busy_e11"
    val emptyView = "test_peak_survives_empty_window_empty_e11"
    val schema = StructType(
      Array(
        StructField("event_id", StringType, nullable   = false),
        StructField("event_name", StringType, nullable = false)
      )
    )
    val rows = NonEmptyList.of(Row("e1", "page_view"), Row("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    LakeWriter.build[IO](config.spark, config.output.good, respectIgluNullability = true, cores = 4).use { lakeWriter =>
      for {
        _ <- lakeWriter.initializeLocalDataFrame(busyView)
        _ <- lakeWriter.localAppendRows(busyView, rows, schema)
        _ <- lakeWriter.prepareCommit(busyView, eventNameCounts)
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
  def e12 = Files[IO].tempDirectory.use { tmpDir =>
    val viewName = "test_no_shuffle_accounting_e12"
    val schema = StructType(
      Array(
        StructField("event_id", StringType, nullable   = false),
        StructField("event_name", StringType, nullable = false)
      )
    )
    val rows = NonEmptyList.of(Row("e1", "page_view"), Row("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    LakeWriter.build[IO](config.spark, config.output.good, respectIgluNullability = true, cores = 2).use { lakeWriter =>
      for {
        _ <- lakeWriter.initializeLocalDataFrame(viewName)
        _ <- lakeWriter.localAppendRows(viewName, rows, schema)
        _ <- lakeWriter.prepareCommit(viewName, eventNameCounts)
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
  def e13 = Files[IO].tempDirectory.use { tmpDir =>
    val firstView  = "test_two_live_shuffles_first_e13"
    val secondView = "test_two_live_shuffles_second_e13"
    val schema = StructType(
      Array(
        StructField("event_id", StringType, nullable   = false),
        StructField("event_name", StringType, nullable = false)
      )
    )
    val rows = NonEmptyList.of(Row("e1", "page_view"), Row("e2", "page_ping"))

    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)

    LakeWriter.build[IO](config.spark, config.output.good, respectIgluNullability = true, cores = 4).use { lakeWriter =>
      for {
        _ <- lakeWriter.initializeLocalDataFrame(firstView)
        _ <- lakeWriter.localAppendRows(firstView, rows, schema)
        _ <- lakeWriter.prepareCommit(firstView, eventNameCounts)
        oneWindow <- lakeWriter.getShuffleDiskBytes
        // A second window reaching prepareCommit before the first is dropped, which is what an
        // overrunning commit produces.
        _ <- lakeWriter.initializeLocalDataFrame(secondView)
        _ <- lakeWriter.localAppendRows(secondView, rows, schema)
        _ <- lakeWriter.prepareCommit(secondView, eventNameCounts)
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

  /**
   * The histogram of the two-row batch every real-Spark example below appends, so `prepareCommit`
   * plans a partitioning over the same keys its rows carry rather than over an empty one.
   */
  val eventNameCounts: Map[Option[String], Int] = Map(Some("page_view") -> 1, Some("page_ping") -> 1)

  sealed trait Action

  object Action {
    case object CreateTableAttempted extends Action
    case class CommitAttempted(viewName: String) extends Action
    case class PrepareCommitAttempted(viewName: String) extends Action
    case class SentAlert(timeSentSeconds: Long) extends Action
    case class BecameUnhealthy(service: RuntimeService) extends Action
    case class BecameHealthy(service: RuntimeService) extends Action
    case object BecameHealthyForSetup extends Action
  }

  sealed trait Response
  object Response {
    case object Success extends Response
    final case class ExceptionThrown(value: Throwable) extends Response
  }

  case class Mocks(lakeWriterResults: List[Response])

  case class Control(
    state: Ref[IO, Vector[Action]],
    lakeWriter: LakeWriter[IO],
    appHealth: AppHealth.Interface[IO, Alert, RuntimeService]
  )

  val retriesConfig = Config.Retries(
    Retrying.Config.ForSetup(30.seconds),
    Retrying.Config.ForTransient(1.second, 5)
  )

  def control(mocks: Mocks = Mocks(Nil)): IO[Control] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      tableManager <- testLakeWriter(state, mocks.lakeWriterResults)
    } yield Control(state, tableManager, testAppHealth(state))

  private def testAppHealth(state: Ref[IO, Vector[Action]]): AppHealth.Interface[IO, Alert, RuntimeService] =
    new AppHealth.Interface[IO, Alert, RuntimeService] {
      def beHealthyForSetup: IO[Unit] =
        state.update(_ :+ Action.BecameHealthyForSetup)
      def beUnhealthyForSetup(alert: Alert): IO[Unit] =
        for {
          now <- IO.realTime
          _ <- state.update(_ :+ Action.SentAlert(now.toSeconds))
        } yield ()
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        state.update(_ :+ Action.BecameHealthy(service))
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        state.update(_ :+ Action.BecameUnhealthy(service))
    }

  private val dummyDestinationSetupErrorCheck: PartialFunction[Throwable, String] = PartialFunction.empty

  private def testLakeWriter(state: Ref[IO, Vector[Action]], mocks: List[Response]): IO[LakeWriter[IO]] =
    for {
      mocksRef <- Ref[IO].of(mocks)
    } yield new LakeWriter[IO] {
      def createTable: IO[Unit] =
        for {
          response <- mocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.CreateTableAttempted)
          result <- response match {
                      case Response.Success =>
                        IO.unit
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { case t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result

      def initializeLocalDataFrame(viewName: String): IO[Unit] = IO.unit

      def localAppendRows(
        viewName: String,
        rows: NonEmptyList[Row],
        schema: StructType
      ): IO[Unit] = IO.unit

      def removeDataFrameFromDisk(viewName: String): IO[Unit] = IO.unit

      def commit(viewName: String): IO[Unit] =
        for {
          response <- mocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.CommitAttempted(viewName))
          result <- response match {
                      case Response.Success =>
                        IO.unit
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { case t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result

      def prepareCommit(viewName: String, eventNameCounts: Map[Option[String], Int]): IO[Unit] =
        for {
          response <- mocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.PrepareCommitAttempted(viewName))
          result <- response match {
                      case Response.Success =>
                        IO.unit
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { case t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result

      def getTableDataFilesTotal: IO[Option[Long]] = IO(Some(123L))

      def getTableSnapshotsRetained: IO[Option[Long]] = IO(Some(456L))

      def getShuffleDiskBytes: IO[Long] = IO.pure(789L)

      def getStorageUsage(viewName: String): IO[Option[LakeWriter.BlockManagerUsage]] = IO(Some(LakeWriter.BlockManagerUsage(1011L, 1213L)))

      def getDiskBytes: IO[Option[Long]] = IO(Some(1415L))

      def recordBlockManagerPeak(viewName: String): IO[Unit] = IO.unit
    }

}
