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

import cats.effect.{Async, Resource, Sync}
import cats.implicits._
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

trait Metrics[F[_]] {
  def addReceived(count: Long): F[Unit]
  def addBad(count: Long): F[Unit]
  def addCommitted(count: Long): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setProcessingLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(latency: FiniteDuration): F[Unit]
  def setTableDataFilesTotal(count: Long): F[Unit]
  def setTableSnapshotsRetained(count: Long): F[Unit]

  def scrape: F[String]
  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](
    config: Config.Metrics,
    sourceAndAck: SourceAndAck[F]
  ): Resource[F, Metrics[F]] =
    CommonMetrics.build(config.statsd, config.prometheus).evalMap { entries =>
      for {
        receivedCounter <- entries.counter("events_received")
        badCounter <- entries.counter("events_bad")
        committedCounter <- entries.counter("events_committed")
        latencyTimer <- entries.timer("latency_millis", sourceAndAck.currentStreamLatency)
        processingLatencyTimer <- entries.timer("processing_latency_millis", Sync[F].pure(None))
        e2eLatencyTimer <- entries.timer("e2e_latency_millis", Sync[F].pure(None))
        tableDataFilesGauge <- entries.gauge("table_data_files_total")
        tableSnapshotsGauge <- entries.gauge("table_snapshots_retained")
      } yield new Metrics[F] {
        def addReceived(count: Long): F[Unit]                      = receivedCounter.add(count)
        def addBad(count: Long): F[Unit]                           = badCounter.add(count)
        def addCommitted(count: Long): F[Unit]                     = committedCounter.add(count)
        def setLatency(latency: FiniteDuration): F[Unit]           = latencyTimer.record(latency)
        def setProcessingLatency(latency: FiniteDuration): F[Unit] = processingLatencyTimer.record(latency)
        def setE2ELatency(latency: FiniteDuration): F[Unit]        = e2eLatencyTimer.record(latency)
        def setTableDataFilesTotal(count: Long): F[Unit]           = tableDataFilesGauge.set(count)
        def setTableSnapshotsRetained(count: Long): F[Unit]        = tableSnapshotsGauge.set(count)

        def scrape: F[String]          = entries.scrape
        def report: Stream[F, Nothing] = entries.report
      }
    }
}
