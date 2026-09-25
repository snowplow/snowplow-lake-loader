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

import cats.effect.{Async, Resource}
import cats.implicits._
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}
import com.snowplowanalytics.snowplow.runtime.Metrics.Destination.{PrometheusAndStatsd, PrometheusOnly}

trait Metrics[F[_]] {
  def addReceived(count: Long): F[Unit]
  def addBad(count: Long): F[Unit]
  def addCommitted(count: Long): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setProcessingLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(latency: FiniteDuration): F[Unit]
  def setTableDataFilesTotal(count: Long): F[Unit]
  def setTableSnapshotsRetained(count: Long): F[Unit]
  def setShuffleDiskBytes(bytes: Long): F[Unit]
  def setStorageMemoryBytes(bytes: Long): F[Unit]
  def setStorageDiskBytes(bytes: Long): F[Unit]
  def setDiskBytes(bytes: Long): F[Unit]

  def scrape: F[String]
  def report: Stream[F, Nothing]
}

object Metrics {

  /**
   * @param windowing
   *   The configured window length, used as the expected-latency hint for the two timers below.
   *   Both measure from a point at or before the window's start to the end of its commit, so
   *   neither can read lower than one window, and the loader's latency moves with this setting
   *   rather than sitting at any fixed value.
   */
  def build[F[_]: Async](
    config: Config.Metrics,
    windowing: FiniteDuration,
    sourceAndAck: SourceAndAck[F]
  ): Resource[F, Metrics[F]] =
    CommonMetrics.build(config.statsd, config.prometheus).evalMap { entries =>
      for {
        receivedCounter <- entries.counter("events_received", PrometheusAndStatsd)
        badCounter <- entries.counter("events_bad", PrometheusAndStatsd)
        committedCounter <- entries.counter("events_committed", PrometheusAndStatsd)
        latencyGauge <- entries.lagGauge("latency", sourceAndAck.currentStreamLatency, PrometheusAndStatsd)
        processingLatencyTimer <- entries.timer("processing_latency", windowing, PrometheusAndStatsd)
        e2eLatencyTimer <- entries.timer("e2e_latency", windowing, PrometheusAndStatsd)
        tableDataFilesGauge <- entries.gauge("table_data_files_total", PrometheusAndStatsd)
        tableSnapshotsGauge <- entries.gauge("table_snapshots_retained", PrometheusAndStatsd)
        // Prefixed, unlike the metrics above, because these report on Spark's internal state rather
        // than on anything in the loader's own domain. They are prometheus-only, so they are absent
        // from statsd and from the periodic stdout report, and are seen only by scraping /metrics.
        shuffleDiskBytesGauge <- entries.gauge("spark_shuffle_disk_bytes", PrometheusOnly)
        storageMemoryBytesGauge <- entries.gauge("spark_storage_memory_bytes", PrometheusOnly)
        storageDiskBytesGauge <- entries.gauge("spark_storage_disk_bytes", PrometheusOnly)
        diskBytesGauge <- entries.gauge("spark_disk_bytes", PrometheusOnly)
      } yield new Metrics[F] {
        def addReceived(count: Long): F[Unit]                      = receivedCounter.add(count)
        def addBad(count: Long): F[Unit]                           = badCounter.add(count)
        def addCommitted(count: Long): F[Unit]                     = committedCounter.add(count)
        def setLatency(latency: FiniteDuration): F[Unit]           = latencyGauge.record(latency)
        def setProcessingLatency(latency: FiniteDuration): F[Unit] = processingLatencyTimer.record(latency)
        def setE2ELatency(latency: FiniteDuration): F[Unit]        = e2eLatencyTimer.record(latency)
        def setTableDataFilesTotal(count: Long): F[Unit]           = tableDataFilesGauge.set(count)
        def setTableSnapshotsRetained(count: Long): F[Unit]        = tableSnapshotsGauge.set(count)
        def setShuffleDiskBytes(bytes: Long): F[Unit]              = shuffleDiskBytesGauge.set(bytes)
        def setStorageMemoryBytes(bytes: Long): F[Unit]            = storageMemoryBytesGauge.set(bytes)
        def setStorageDiskBytes(bytes: Long): F[Unit]              = storageDiskBytesGauge.set(bytes)
        def setDiskBytes(bytes: Long): F[Unit]                     = diskBytesGauge.set(bytes)

        def scrape: F[String]          = entries.scrape
        def report: Stream[F, Nothing] = entries.report
      }
    }
}
