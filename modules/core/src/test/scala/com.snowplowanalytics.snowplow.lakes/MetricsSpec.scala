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
import cats.effect.testing.specs2.CatsEffect
import fs2.Stream
import org.specs2.Specification

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, SourceAndAck}

// The gauge names are an external contract - dashboards and alerts are written against them - and
// nothing between `Metrics.build` and the scrape endpoint is under our control: micrometer decides
// what a meter is called in Prometheus exposition format. So assert on the scrape output rather
// than on the strings passed to `entries.gauge`.
class MetricsSpec extends Specification with CatsEffect {

  def is = s2"""
  The scrape endpoint should:
    expose the spark usage gauges under the names they were registered with $e1
    expose the latency metrics with the unit suffix prometheus gives them $e2
  """

  def e1 =
    Metrics
      .build[IO](Config.Metrics(statsd = None, prometheus = CommonMetrics.PrometheusConfig(tags = Map.empty)), 5.minutes, dummySource)
      .use { metrics =>
        for {
          _ <- metrics.setShuffleDiskBytes(111L)
          _ <- metrics.setStorageMemoryBytes(222L)
          _ <- metrics.setStorageDiskBytes(333L)
          _ <- metrics.setDiskBytes(444L)
          scraped <- metrics.scrape
        } yield scraped.linesIterator.filterNot(_.startsWith("#")).map(_.trim).toSet must containAllOf(
          List(
            "spark_shuffle_disk_bytes 111.0",
            "spark_storage_memory_bytes 222.0",
            "spark_storage_disk_bytes 333.0",
            "spark_disk_bytes 444.0"
          )
        )
      }

  // common-streams appends the unit suffix itself, so the loader registers bare names and the
  // series that reaches prometheus is not the string in `Metrics.build`.
  def e2 =
    Metrics
      .build[IO](Config.Metrics(statsd = None, prometheus = CommonMetrics.PrometheusConfig(tags = Map.empty)), 5.minutes, dummySource)
      .use { metrics =>
        for {
          _ <- metrics.setLatency(7.seconds)
          _ <- metrics.setProcessingLatency(5.minutes)
          _ <- metrics.setE2ELatency(6.minutes)
          scraped <- metrics.scrape
        } yield {
          val names = scraped.linesIterator.filterNot(_.startsWith("#")).map(_.trim.takeWhile(c => c != ' ' && c != '{')).toSet
          names must containAllOf(
            List(
              "latency_seconds",
              "latency_observations_total",
              "processing_latency_seconds_count",
              "e2e_latency_seconds_count"
            )
          )
        }
      }

  private def dummySource: SourceAndAck[IO] = new SourceAndAck[IO] {
    def stream(config: EventProcessingConfig[IO], processor: EventProcessor[IO]): Stream[IO, Nothing] = Stream.never[IO]
    def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus]         = IO.pure(SourceAndAck.Healthy)
    def currentStreamLatency: IO[Option[FiniteDuration]]                                              = IO.pure(None)
  }
}
