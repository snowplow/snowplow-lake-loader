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

import cats.effect.IO
import cats.effect.kernel.Resource
import org.http4s.client.Client
import fs2.Stream
import fs2.io.file.Path

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.streams.{
  EventProcessingConfig,
  EventProcessor,
  ListOfList,
  Sink,
  Sinkable,
  SourceAndAck,
  TokenedEvents
}
import com.snowplowanalytics.snowplow.lakes.processing.LakeWriter
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, Retrying}

object TestSparkEnvironment {

  def build(
    target: TestConfig.Target,
    tmpDir: Path,
    windows: List[List[TokenedEvents]]
  ): Resource[IO, Environment[IO]] = for {
    testConfig <- Resource.pure(TestConfig.defaults(target, tmpDir))
    source = testSourceAndAck(windows)
    lakeWriter <- LakeWriter.build[IO](testConfig.spark, testConfig.output.good)
    lakeWriterWrapped = LakeWriter.withHandledErrors(lakeWriter, dummyAppHealth, retriesConfig, PartialFunction.empty)
  } yield Environment(
    appInfo = appInfo,
    source  = source,
    badSink = new Sink[IO] {
      def isHealthy: IO[Boolean]                      = IO.pure(true)
      def sink(batch: ListOfList[Sinkable]): IO[Unit] = IO.unit
    },
    resolver                = Resolver[IO](Nil, None),
    httpClient              = testHttpClient,
    lakeWriter              = lakeWriterWrapped,
    metrics                 = testMetrics,
    appHealth               = dummyAppHealth,
    inMemBatchBytes         = 1000000L,
    cpuParallelism          = 1,
    windowing               = EventProcessingConfig.TimedWindows(1.minute, 1.0, 1),
    badRowMaxSize           = 1000000,
    schemasToSkip           = List.empty,
    respectIgluNullability  = true,
    exitOnMissingIgluSchema = false
  )

  private val retriesConfig = Config.Retries(
    Retrying.Config.ForSetup(30.seconds),
    Retrying.Config.ForTransient(1.second, 5)
  )

  private def testSourceAndAck(windows: List[List[TokenedEvents]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig[IO], processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream.emits(windows).flatMap { batches =>
          Stream
            .emits(batches)
            .through(processor)
            .drain
        }

      def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)

      def currentStreamLatency: IO[Option[FiniteDuration]] =
        IO.pure(None)
    }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  val appInfo = new AppInfo {
    def name        = "lake-loader-test"
    def version     = "0.0.0"
    def dockerAlias = "snowplow/lake-loader-test:0.0.0"
    def cloud       = "OnPrem"
  }

  def testMetrics: Metrics[IO] = new Metrics[IO] {
    def addReceived(count: Int): IO[Unit]                       = IO.unit
    def addBad(count: Int): IO[Unit]                            = IO.unit
    def addCommitted(count: Int): IO[Unit]                      = IO.unit
    def setLatency(latency: FiniteDuration): IO[Unit]           = IO.unit
    def setProcessingLatency(latency: FiniteDuration): IO[Unit] = IO.unit
    def setE2ELatency(latency: FiniteDuration): IO[Unit]        = IO.unit
    def setTableDataFilesTotal(count: Long): IO[Unit]           = IO.unit
    def setTableSnapshotsRetained(count: Long): IO[Unit]        = IO.unit
    def report: Stream[IO, Nothing]                             = Stream.never[IO]
  }

  def dummyAppHealth: AppHealth.Interface[IO, Any, Any] = new AppHealth.Interface[IO, Any, Any] {
    def beHealthyForSetup: IO[Unit] =
      IO.unit
    def beUnhealthyForSetup(alert: Any): IO[Unit] =
      IO.unit
    def beHealthyForRuntimeService(service: Any): IO[Unit] =
      IO.unit
    def beUnhealthyForRuntimeService(service: Any): IO[Unit] =
      IO.unit
  }

}
