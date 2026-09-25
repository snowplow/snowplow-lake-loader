/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}
import org.http4s.client.Client
import fs2.Stream

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.runtime.AppHealth
import com.snowplowanalytics.snowplow.streams.{
  EventProcessingConfig,
  EventProcessor,
  ListOfList,
  Sink,
  Sinkable,
  SourceAndAck,
  TokenedEvents
}
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig
import com.snowplowanalytics.snowplow.lakes.processing.LakeWriter

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  /** All tests can use the same window duration */
  val WindowDuration = 42.seconds

  /** All tests can use the same time taken to create table */
  val TimeTakenToCreateTable = 10.seconds

  sealed trait Action
  object Action {
    case object SubscribedToStream extends Action
    case object CreatedTable extends Action
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Int) extends Action
    case class InitializedLocalDataFrame(viewName: String) extends Action
    case class AppendedRowsToDataFrame(viewName: String, numEvents: Int) extends Action
    case class RemovedDataFrameFromDisk(viewName: String) extends Action
    case class CommittedToTheLake(viewName: String) extends Action

    /**
     * The histogram is on the action because it is the only thing that pins `countEventNames`: it
     * otherwise reaches nothing assertable, since a wrong partition assignment is a slow commit
     * rather than a failure.
     */
    case class PreparedCommit(viewName: String, eventNameCounts: Map[Option[String], Int]) extends Action

    /* Metrics */
    case class AddedReceivedCountMetric(count: Long) extends Action
    case class AddedBadCountMetric(count: Long) extends Action
    case class AddedCommittedCountMetric(count: Long) extends Action
    case class SetLatencyMetric(latency: FiniteDuration) extends Action
    case class SetProcessingLatencyMetric(latency: FiniteDuration) extends Action
    case class SetE2ELatencyMetric(latency: FiniteDuration) extends Action
    case class SetTableDataFilesTotal(count: Long) extends Action
    case class SetTableSnaphotsRetained(count: Long) extends Action
    case class SetShuffleDiskBytes(bytes: Long) extends Action
    case class SetStorageMemoryBytes(bytes: Long) extends Action
    case class SetStorageDiskBytes(bytes: Long) extends Action
    case class SetDiskBytes(bytes: Long) extends Action

    /* Health */
    case class BecameUnhealthy(service: RuntimeService) extends Action
    case class BecameHealthy(service: RuntimeService) extends Action
  }
  import Action._

  /**
   * Build a mock environment for testing
   *
   * @param windows
   *   Input events to send into the environment. The outer List represents a timed window of
   *   events; the inner list represents batches of events within the window
   * @return
   *   An environment and a Ref that records the actions make by the environment
   */
  def build(
    windows: List[List[TokenedEvents]],
    diskBytes: Option[Long]                            = Some(1415L),
    storageUsage: Option[LakeWriter.BlockManagerUsage] = Some(LakeWriter.BlockManagerUsage(1011L, 1213L)),
    sparkUsageReadFails: Boolean                       = false
  ): IO[MockEnvironment] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      source = testSourceAndAck(windows, state)
    } yield {
      val env = Environment(
        appInfo                 = TestSparkEnvironment.appInfo,
        source                  = source,
        badSink                 = testSink(state),
        resolver                = Resolver[IO](Nil, None),
        httpClient              = testHttpClient,
        lakeWriter              = testLakeWriter(state, diskBytes, storageUsage, sparkUsageReadFails),
        metrics                 = testMetrics(state),
        appHealth               = testAppHealth(state),
        inMemBatchBytes         = 1000000L,
        cpuParallelism          = 1,
        windowing               = EventProcessingConfig.TimedWindows(1.minute, 1.0, 1),
        badRowMaxSize           = 1000000,
        schemasToSkip           = List.empty,
        respectIgluNullability  = true,
        exitOnMissingIgluSchema = false,
        decompression           = DecompressionConfig(maxBytesInBatch = 5242880, maxBytesSinglePayload = 10000000)
      )
      MockEnvironment(state, env)
    }

  private def testLakeWriter(
    state: Ref[IO, Vector[Action]],
    diskBytes: Option[Long],
    storageUsage: Option[LakeWriter.BlockManagerUsage],
    sparkUsageReadFails: Boolean
  ): LakeWriter.WithHandledErrors[IO] = new LakeWriter.WithHandledErrors[IO] {
    def createTable: IO[Unit] =
      IO.sleep(TimeTakenToCreateTable) *> state.update(_ :+ CreatedTable)

    def initializeLocalDataFrame(viewName: String): IO[Unit] =
      state.update(_ :+ InitializedLocalDataFrame(viewName))

    def localAppendRows(
      viewName: String,
      rows: NonEmptyList[Row],
      schema: StructType
    ): IO[Unit] =
      state.update(_ :+ AppendedRowsToDataFrame(viewName, rows.size))

    def removeDataFrameFromDisk(viewName: String): IO[Unit] =
      state.update(_ :+ RemovedDataFrameFromDisk(viewName))

    def prepareCommit(viewName: String, eventNameCounts: Map[Option[String], Int]): IO[Unit] =
      state.update(_ :+ PreparedCommit(viewName, eventNameCounts))

    def commit(viewName: String): IO[Unit] =
      state.update(_ :+ CommittedToTheLake(viewName))

    def getTableDataFilesTotal: IO[Option[Long]] = IO(Some(123L))

    def getTableSnapshotsRetained: IO[Option[Long]] = IO(Some(456L))

    def getShuffleDiskBytes: IO[Long] =
      if (sparkUsageReadFails) IO.raiseError(new RuntimeException("boom reading shuffle disk bytes")) else IO.pure(789L)

    def getStorageUsage(viewName: String): IO[Option[LakeWriter.BlockManagerUsage]] = IO.pure(storageUsage)

    def getDiskBytes: IO[Option[Long]] = IO.pure(diskBytes)

    def recordBlockManagerPeak(viewName: String): IO[Unit] = IO.unit
  }

  private def testSourceAndAck(windows: List[List[TokenedEvents]], state: Ref[IO, Vector[Action]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig[IO], processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream.eval(state.update(_ :+ SubscribedToStream)).drain ++
          Stream.emits(windows).flatMap { batches =>
            Stream
              .emits(batches)
              .onFinalize(IO.sleep(WindowDuration))
              .through(processor)
              .chunks
              .evalMap { chunk =>
                state.update(_ :+ Checkpointed(chunk.toList))
              }
              .drain
          }

      def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)

      def currentStreamLatency: IO[Option[FiniteDuration]] =
        IO.pure(None)
    }

  private def testSink(ref: Ref[IO, Vector[Action]]): Sink[IO] = new Sink[IO] {
    def isHealthy: IO[Boolean] = IO.pure(true)
    def sink(batch: ListOfList[Sinkable]): IO[Unit] =
      ref.update(_ :+ SentToBad(batch.asIterable.size))
  }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] = new Metrics[IO] {
    def addReceived(count: Long): IO[Unit] =
      ref.update(_ :+ AddedReceivedCountMetric(count))

    def addBad(count: Long): IO[Unit] =
      ref.update(_ :+ AddedBadCountMetric(count))

    def addCommitted(count: Long): IO[Unit] =
      ref.update(_ :+ AddedCommittedCountMetric(count))

    def setLatency(latency: FiniteDuration): IO[Unit] =
      ref.update(_ :+ SetLatencyMetric(latency))

    def setProcessingLatency(latency: FiniteDuration): IO[Unit] =
      ref.update(_ :+ SetProcessingLatencyMetric(latency))

    def setE2ELatency(latency: FiniteDuration): IO[Unit] =
      ref.update(_ :+ SetE2ELatencyMetric(latency))

    def setTableDataFilesTotal(count: Long): IO[Unit] =
      ref.update(_ :+ SetTableDataFilesTotal(count))

    def setTableSnapshotsRetained(count: Long): IO[Unit] =
      ref.update(_ :+ SetTableSnaphotsRetained(count))

    def setShuffleDiskBytes(bytes: Long): IO[Unit] =
      ref.update(_ :+ SetShuffleDiskBytes(bytes))

    def setStorageMemoryBytes(bytes: Long): IO[Unit] =
      ref.update(_ :+ SetStorageMemoryBytes(bytes))

    def setStorageDiskBytes(bytes: Long): IO[Unit] =
      ref.update(_ :+ SetStorageDiskBytes(bytes))

    def setDiskBytes(bytes: Long): IO[Unit] =
      ref.update(_ :+ SetDiskBytes(bytes))

    def scrape: IO[String] = IO.pure("")

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }

  private def testAppHealth(ref: Ref[IO, Vector[Action]]): AppHealth.Interface[IO, Alert, RuntimeService] =
    new AppHealth.Interface[IO, Alert, RuntimeService] {
      def beHealthyForSetup: IO[Unit] =
        IO.unit
      def beUnhealthyForSetup(alert: Alert): IO[Unit] =
        IO.unit
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(_ :+ BecameHealthy(service))
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(_ :+ BecameUnhealthy(service))
    }
}
