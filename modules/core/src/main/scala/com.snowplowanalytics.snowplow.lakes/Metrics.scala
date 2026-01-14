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

import cats.Functor
import cats.effect.Async
import cats.effect.kernel.Ref
import cats.implicits._
import fs2.Stream

import scala.concurrent.duration.{Duration, FiniteDuration}

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

trait Metrics[F[_]] {
  def addReceived(count: Int): F[Unit]
  def addBad(count: Int): F[Unit]
  def addCommitted(count: Int): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setProcessingLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(latency: FiniteDuration): F[Unit]
  def setTableDataFilesTotal(count: Long): F[Unit]
  def setTableSnapshotsRetained(count: Long): F[Unit]

  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics, sourceAndAck: SourceAndAck[F]): F[Metrics[F]] =
    Ref.ofEffect(State.initialize(sourceAndAck)).map(impl(config, _, sourceAndAck))

  private case class State(
    received: Int,
    bad: Int,
    committed: Int,
    latency: FiniteDuration,
    processingLatency: Option[FiniteDuration],
    e2eLatency: Option[FiniteDuration],
    tableDataFilesTotal: Option[Long],
    tableSnapshotsRetained: Option[Long]
  ) extends CommonMetrics.State {
    def toKVMetrics: List[CommonMetrics.KVMetric] =
      List(
        KVMetric.CountReceived(received),
        KVMetric.CountBad(bad),
        KVMetric.CountCommitted(committed),
        KVMetric.Latency(latency)
      ) ++ processingLatency.map(KVMetric.ProcessingLatency(_)) ++ e2eLatency.map(KVMetric.E2ELatency(_)) ++
        tableDataFilesTotal.map(KVMetric.TableDataFilesTotal(_)) ++ tableSnapshotsRetained.map(KVMetric.TableSnaphotsRetained(_))
  }

  private object State {
    def initialize[F[_]: Functor](sourceAndAck: SourceAndAck[F]): F[State] =
      sourceAndAck.currentStreamLatency.map { latency =>
        State(0, 0, 0, latency.getOrElse(Duration.Zero), None, None, None, None)
      }
  }

  private def impl[F[_]: Async](
    config: Config.Metrics,
    ref: Ref[F, State],
    sourceAndAck: SourceAndAck[F]
  ): Metrics[F] =
    new CommonMetrics[F, State](ref, State.initialize(sourceAndAck), config.statsd) with Metrics[F] {
      def addReceived(count: Int): F[Unit] =
        ref.update(s => s.copy(received = s.received + count))
      def addBad(count: Int): F[Unit] =
        ref.update(s => s.copy(bad = s.bad + count))
      def addCommitted(count: Int): F[Unit] =
        ref.update(s => s.copy(committed = s.committed + count))
      def setLatency(latency: FiniteDuration): F[Unit] =
        ref.update(s => s.copy(latency = s.latency.max(latency)))
      def setProcessingLatency(latency: FiniteDuration): F[Unit] =
        ref.update { state =>
          val newLatency = state.processingLatency.fold(latency)(_.max(latency))
          state.copy(processingLatency = Some(newLatency))
        }
      def setE2ELatency(latency: FiniteDuration): F[Unit] =
        ref.update { state =>
          val newLatency = state.e2eLatency.fold(latency)(_.max(latency))
          state.copy(e2eLatency = Some(newLatency))
        }
      def setTableDataFilesTotal(count: Long): F[Unit] =
        ref.update { state =>
          val newCount = state.tableDataFilesTotal.fold(count)(_.max(count))
          state.copy(tableDataFilesTotal = Some(newCount))
        }
      def setTableSnapshotsRetained(count: Long): F[Unit] =
        ref.update { state =>
          val newCount = state.tableSnapshotsRetained.fold(count)(_.max(count))
          state.copy(tableSnapshotsRetained = Some(newCount))
        }
    }

  private object KVMetric {

    final case class CountReceived(v: Int) extends CommonMetrics.KVMetric {
      val key        = "events_received"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountBad(v: Int) extends CommonMetrics.KVMetric {
      val key        = "events_bad"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountCommitted(v: Int) extends CommonMetrics.KVMetric {
      val key        = "events_committed"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class Latency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "latency_millis"
      val value      = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class ProcessingLatency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "processing_latency_millis"
      val value      = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class E2ELatency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "e2e_latency_millis"
      val value      = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class TableDataFilesTotal(v: Long) extends CommonMetrics.KVMetric {
      val key        = "table_data_files_total"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class TableSnaphotsRetained(v: Long) extends CommonMetrics.KVMetric {
      val key        = "table_snapshots_retained"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }
  }
}
