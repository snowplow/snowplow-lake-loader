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

import cats.implicits._
import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._
import com.comcast.ip4s.Port

import java.net.URI
import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, HttpClient, Metrics => CommonMetrics, Retrying, Sentry, Telemetry, Webhook}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs.schemaCriterionDecoder
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig

case class Config[+Factory, +Source, +Sink](
  streams: Factory,
  input: Source,
  output: Config.Output[Sink],
  inMemBatchBytes: Long,
  cpuParallelismFraction: BigDecimal,
  numEagerWindows: Int,
  windowing: FiniteDuration,
  spark: Config.Spark,
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring,
  license: AcceptedLicense,
  skipSchemas: List[SchemaCriterion],
  respectIgluNullability: Boolean,
  exitOnMissingIgluSchema: Boolean,
  entitySupersetCapacity: Int,
  retries: Config.Retries,
  http: Config.Http,
  decompression: DecompressionConfig
)

object Config {

  case class WithIglu[+Factory, +Source, +Sink](main: Config[Factory, Source, Sink], iglu: ResolverConfig)

  case class Output[+Sink](good: Target, bad: SinkWithMaxSize[Sink])

  case class SinkWithMaxSize[+Sink](sink: Sink, maxRecordSize: Int)

  case class MaxRecordSize(maxRecordSize: Int)

  sealed trait Target {

    /** The column the table is partitioned by, alongside the date of `load_tstamp` */
    def tablePartitionColumn: Option[PartitionColumn]
  }

  /**
   * @param partitionColumn
   *   `None`, from a HOCON `null`, for a table with no partitioning, which the loader creates
   *   clustered by `load_tstamp`. `reference.conf` sets `event_name`, so leaving the key out does
   *   not mean `None`.
   */
  case class Delta(
    location: URI,
    deltaTableProperties: Map[String, String],
    partitionColumn: Option[PartitionColumn]
  ) extends Target {
    def tablePartitionColumn: Option[PartitionColumn] = partitionColumn
  }

  /**
   * @param partitionColumn
   *   The loader always partitions an Iceberg table, so there is no `null` here
   */
  case class Iceberg(
    database: String,
    table: String,
    catalog: IcebergCatalog,
    location: Option[URI],
    icebergTableProperties: Map[String, String],
    icebergWriteOptions: Map[String, String],
    partitionColumn: PartitionColumn
  ) extends Target {
    def tablePartitionColumn: Option[PartitionColumn] = Some(partitionColumn)
  }

  /**
   * The column the table is partitioned by, alongside the date of `load_tstamp`.
   *
   * It decides the partition spec only when the loader creates the table. An existing table must
   * already be partitioned this way, and stay so while the loader runs: the config is
   * authoritative, and the loader fails rather than adopt the table's spec.
   */
  sealed trait PartitionColumn {
    def name: String
  }

  object PartitionColumn {
    case object EventName extends PartitionColumn {
      val name = "event_name"
    }

    case object AppId extends PartitionColumn {
      val name = "app_id"
    }

    val all: List[PartitionColumn] = List(EventName, AppId)

    implicit val decoder: Decoder[PartitionColumn] =
      Decoder.decodeString.emap { s =>
        all.find(_.name === s).toRight(s"Unsupported partitionColumn $s. Must be one of: ${all.map(_.name).mkString(", ")}")
      }
  }

  sealed trait IcebergCatalog

  object IcebergCatalog {

    case class Hadoop(options: Map[String, String]) extends IcebergCatalog

    case class Glue(
      options: Map[String, String]
    ) extends IcebergCatalog

    case class Rest(
      uri: URI,
      name: String,
      options: Map[String, String]
    ) extends IcebergCatalog

  }

  case class Spark(
    taskRetries: Int,
    writerPartitioning: WriterPartitioning,
    conf: Map[String, String]
  )

  /**
   * How the loader spreads a window's events over the Spark partitions that write to the lake.
   *
   * The partition count is always `writerParallelism` and is not configurable, because one Spark
   * task slot must be left free for the per-batch handover. See `processing.WriterPartitioner`.
   *
   * Written in terms of event names; the same applies to app ids when `app_id` is the partition
   * column.
   *
   * @param splitsPerFairShare
   *   How finely to cut an event name, as a fraction of a fair share: the loader aims for pieces of
   *   `fairShare / splitsPerFairShare` events. Smaller pieces give the bin-packer finer-grained
   *   items to balance the partitions with, at the cost of an output file per extra piece.
   * @param minEventsPerSplit
   *   A hard floor on how small a piece may be. It caps the number of pieces rather than their
   *   target size, so no piece ever holds fewer events than this - and an event name is therefore
   *   split at all only once it has at least twice this many events. Without it, splitting is
   *   purely relative to a fair share, so a large loader receiving a slow trickle would cut a
   *   handful of events apart and write a parquet file for each one.
   *
   * `WriterPartitioner` splits until no item is larger than a piece, so greedy packing leaves the
   * heaviest partition within one piece of a fair share - but only where it takes the split plan,
   * which it declines when the balance it buys does not repay the output files. Both settings
   * therefore trade files for a tighter commit rather than buying balance outright.
   *
   * The floor is what an event name below twice it cannot be split, so a window of a few such names
   * can spread unevenly over the partitions and no setting changes that. That is the trade the
   * floor exists to make: such a window is small enough to commit well inside its window whatever
   * the spread, and whole output files are worth more there than even writer threads.
   *
   * For an unpartitioned table only `minEventsPerSplit` applies: the window is spread by a hash of
   * `event_id` over as many partitions as give each about `minEventsPerSplit` events, up to
   * `writerParallelism`.
   */
  case class WriterPartitioning(
    splitsPerFairShare: Int,
    minEventsPerSplit: Int
  )

  case class Metrics(
    statsd: Option[CommonMetrics.StatsdConfig],
    prometheus: CommonMetrics.PrometheusConfig
  )

  case class HealthProbe(port: Port, unhealthyLatency: FiniteDuration)

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry.Config],
    healthProbe: HealthProbe,
    webhook: Webhook.Config
  )

  case class SetupErrorRetries(delay: FiniteDuration)
  case class TransientErrorRetries(delay: FiniteDuration, attempts: Int)

  case class Retries(
    setupErrors: Retrying.Config.ForSetup,
    transientErrors: Retrying.Config.ForTransient
  )

  case class Http(client: HttpClient.Config)

  implicit def decoder[Factory: Decoder, Source: Decoder, Sink: Decoder]: Decoder[Config[Factory, Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val sinkWithMaxSize = for {
      sink <- Decoder[Sink]
      maxSize <- deriveConfiguredDecoder[MaxRecordSize]
    } yield SinkWithMaxSize(sink, maxSize.maxRecordSize)
    implicit val icebergCatalog     = deriveConfiguredDecoder[IcebergCatalog]
    implicit val target             = deriveConfiguredDecoder[Target]
    implicit val output             = deriveConfiguredDecoder[Output[Sink]]
    implicit val writerPartitioning = deriveConfiguredDecoder[WriterPartitioning]
    implicit val spark              = deriveConfiguredDecoder[Spark]
    implicit val sentryDecoder      = Sentry.ConfigM.sentryDecoder
    implicit val metricsDecoder     = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val monitoringDecoder  = deriveConfiguredDecoder[Monitoring]
    implicit val retriesDecoder     = deriveConfiguredDecoder[Retries]
    implicit val httpDecoder        = deriveConfiguredDecoder[Http]

    // TODO add specific lake-loader docs for license
    implicit val licenseDecoder =
      AcceptedLicense.decoder(AcceptedLicense.DocumentationLink("https://docs.snowplow.io/limited-use-license-1.1/"))

    deriveConfiguredDecoder[Config[Factory, Source, Sink]]
  }

}
