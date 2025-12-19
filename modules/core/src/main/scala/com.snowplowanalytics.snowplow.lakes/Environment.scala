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
import cats.effect.{Async, Resource, Sync}
import org.http4s.client.Client
import io.sentry.Sentry

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.lakes.processing.LakeWriter
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, HealthProbe, HttpClient, Webhook}

/**
 * Resources and runtime-derived configuration needed for processing events
 *
 * @param cpuParallelism
 *   The processing Pipe involves several steps, some of which are cpu-intensive. We run
 *   cpu-intensive steps in parallel, so that on big instances we can take advantage of all cores.
 *   For each of those cpu-intensive steps, `cpuParallelism` controls the parallelism of that step.
 * @param inMemBatchBytes
 *   The processing Pipe batches up Events in memory, and then flushes them to local disk to avoid
 *   running out memory. This param limits the maximum size of a batch. Note, because of how the
 *   loader works on batches *in parallel*, it is possible for the loader to be holding more than
 *   this number of bytes in memory at any one time.
 *
 * Other params are self-explanatory
 */
case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  resolver: Resolver[F],
  httpClient: Client[F],
  lakeWriter: LakeWriter.WithHandledErrors[F],
  metrics: Metrics[F],
  appHealth: AppHealth.Interface[F, Alert, RuntimeService],
  cpuParallelism: Int,
  inMemBatchBytes: Long,
  windowing: EventProcessingConfig.TimedWindows,
  badRowMaxSize: Int,
  schemasToSkip: List[SchemaCriterion],
  respectIgluNullability: Boolean,
  exitOnMissingIgluSchema: Boolean
)

object Environment {

  def fromConfig[F[_]: Async, FactoryConfig, SourceConfig, SinkConfig](
    config: Config.WithIglu[FactoryConfig, SourceConfig, SinkConfig],
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, SinkConfig]],
    destinationSetupErrorCheck: DestinationSetupErrorCheck
  ): Resource[F, Environment[F]] =
    for {
      _ <- enableSentry[F](appInfo, config.main.monitoring.sentry)
      factory <- toFactory(config.main.streams)
      sourceAndAck <- factory.source(config.main.input)
      sourceReporter = sourceAndAck.isHealthy(config.main.monitoring.healthProbe.unhealthyLatency).map(_.showIfUnhealthy)
      appHealth <- Resource.eval(AppHealth.init[F, Alert, RuntimeService](List(sourceReporter)))
      resolver <- mkResolver[F](config.iglu)
      httpClient <- HttpClient.resource[F](config.main.http.client)
      _ <- HealthProbe.resource(config.main.monitoring.healthProbe.port, appHealth)
      _ <- Webhook.resource(config.main.monitoring.webhook, appInfo, httpClient, appHealth)
      badSink <-
        factory
          .sink(config.main.output.bad.sink)
          .onError(_ => Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)))
      windowing <- Resource.eval(EventProcessingConfig.TimedWindows.build(config.main.windowing, config.main.numEagerWindows))
      lakeWriter <- LakeWriter.build(config.main.spark, config.main.output.good)
      destinationAndTableFormatSetupErrorCheck = destinationSetupErrorCheck.orElse(TableFormatSetupError.check(config.main.output.good))
      lakeWriterWrapped = LakeWriter.withHandledErrors(lakeWriter, appHealth, config.main.retries, destinationAndTableFormatSetupErrorCheck)
      metrics <- Resource.eval(Metrics.build(config.main.monitoring.metrics, sourceAndAck))
      cpuParallelism = chooseCpuParallelism(config.main)
    } yield Environment(
      appInfo                 = appInfo,
      source                  = sourceAndAck,
      badSink                 = badSink,
      resolver                = resolver,
      httpClient              = httpClient,
      lakeWriter              = lakeWriterWrapped,
      metrics                 = metrics,
      appHealth               = appHealth,
      cpuParallelism          = cpuParallelism,
      inMemBatchBytes         = config.main.inMemBatchBytes,
      windowing               = windowing,
      badRowMaxSize           = config.main.output.bad.maxRecordSize,
      schemasToSkip           = config.main.skipSchemas,
      respectIgluNullability  = config.main.respectIgluNullability,
      exitOnMissingIgluSchema = config.main.exitOnMissingIgluSchema
    )

  private def enableSentry[F[_]: Sync](appInfo: AppInfo, config: Option[Config.Sentry]): Resource[F, Unit] =
    config match {
      case Some(c) =>
        val acquire = Sync[F].delay {
          Sentry.init { options =>
            options.setDsn(c.dsn)
            options.setRelease(appInfo.version)
            c.tags.foreach { case (k, v) =>
              options.setTag(k, v)
            }
          }
        }

        Resource.makeCase(acquire) {
          case (_, Resource.ExitCase.Errored(e)) => Sync[F].delay(Sentry.captureException(e)).void
          case _                                 => Sync[F].unit
        }
      case None =>
        Resource.unit[F]
    }

  private def mkResolver[F[_]: Async](resolverConfig: Resolver.ResolverConfig): Resource[F, Resolver[F]] =
    Resource.eval {
      Resolver
        .fromConfig[F](resolverConfig)
        .leftMap(e => new RuntimeException(s"Error while parsing Iglu resolver config", e))
        .value
        .rethrow
    }

  /*
  private def chooseInMemMaxBytes(config: AnyConfig): Long =
    (Runtime.getRuntime.maxMemory * config.inMemHeapFraction).toLong
   */

  /**
   * See the description of `cpuParallelism` on the [[Environment]] class
   *
   * For bigger instances (more cores) we want more parallelism, so that cpu-intensive steps can
   * take advantage of all the cores.
   */
  private def chooseCpuParallelism(config: AnyConfig): Int =
    (Runtime.getRuntime.availableProcessors * config.cpuParallelismFraction)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

}
