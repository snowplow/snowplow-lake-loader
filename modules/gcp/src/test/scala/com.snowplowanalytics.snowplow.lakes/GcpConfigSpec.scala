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

import cats.Id
import cats.effect.testing.specs2.CatsEffect
import cats.effect.{ExitCode, IO}
import com.comcast.ip4s.Port
import org.http4s.implicits.http4sLiteralsSyntax
import org.specs2.Specification

import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, ConfigParser, HttpClient, Retrying, Sentry, Telemetry, Webhook}
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig
import com.snowplowanalytics.snowplow.streams.pubsub.{PubsubFactoryConfig, PubsubSinkConfig, PubsubSinkConfigM, PubsubSourceConfig}

import java.net.URI
import java.nio.file.Paths
import scala.concurrent.duration.DurationInt

class GcpConfigSpec extends Specification with CatsEffect {

  def is = s2"""
   Config parse should be able to parse
    minimal gcp config $minimal
    reference gcp config $reference
  """

  private def minimal =
    assert(
      resource = "/config.gcp.minimal.hocon",
      expectedResult = Right(
        GcpConfigSpec.minimalConfig
      )
    )

  private def reference =
    assert(
      resource = "/config.gcp.reference.hocon",
      expectedResult = Right(
        GcpConfigSpec.referenceConfig
      )
    )

  private def assert(
    resource: String,
    expectedResult: Either[ExitCode, Config[PubsubFactoryConfig, PubsubSourceConfig, PubsubSinkConfig]]
  ) = {
    val path = Paths.get(getClass.getResource(resource).toURI)
    ConfigParser.configFromFile[IO, Config[PubsubFactoryConfig, PubsubSourceConfig, PubsubSinkConfig]](path).value.map { result =>
      result must beEqualTo(expectedResult)
    }
  }
}

object GcpConfigSpec {

  private val defaultSparkConf = Map(
    "spark.ui.enabled" -> "false",
    "spark.local.dir" -> "/tmp",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.memory.fraction" -> "0.3",
    "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS",
    "spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
    "spark.memory.storageFraction" -> "0",
    "spark.rdd.compress" -> "true",
    "spark.databricks.delta.autoCompact.enabled" -> "false",
    "spark.scheduler.mode" -> "FAIR",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.ansi.enabled" -> "false",
    "fs.s3a.aws.credentials.provider" -> "com.snowplowanalytics.snowplow.lakes.AssumedRoleCredentialsProvider",
    "fs.s3a.assumed.role.session.name" -> "snowplow-lake-loader",
    "fs.s3a.assumed.role.session.duration" -> "1h",
    "spark.io.delta.storage.S3DynamoDBLogStore.credentials.provider" -> "com.snowplowanalytics.snowplow.lakes.AssumedRoleCredentialsProviderV1",
    "fs.azure.account.auth.type" -> "Custom",
    "fs.azure.account.oauth.provider.type" -> "com.snowplowanalytics.snowplow.lakes.AzureTokenProvider"
  )

  private val minimalConfig = Config[PubsubFactoryConfig, PubsubSourceConfig, PubsubSinkConfig](
    streams = PubsubFactoryConfig(emulatorHost = None),
    input = PubsubSourceConfig(
      subscription            = PubsubSourceConfig.Subscription("myproject", "snowplow-enriched"),
      parallelPullFactor      = BigDecimal("2.0"),
      durationPerAckExtension = 10.minutes,
      minRemainingAckDeadline = BigDecimal("0.1"),
      maxMessagesPerPull      = 1000,
      debounceRequests        = 100.millis,
      streamingPull           = false,
      retries                 = PubsubSourceConfig.Retries(PubsubSourceConfig.TransientErrorRetrying(delay = 100.millis, attempts = 10))
    ),
    output = Config.Output(
      good = Config.Delta(
        location = URI.create("gs://my-bucket/events"),
        deltaTableProperties = Map(
          "delta.logRetentionDuration" -> "interval 1 days",
          "delta.deletedFileRetentionDuration" -> "interval 1 days",
          "delta.dataSkippingStatsColumns" -> "load_tstamp,collector_tstamp,derived_tstamp,dvce_created_tstamp,true_tstamp,event_name,app_id",
          "delta.checkpointInterval" -> "50"
        ),
        partitionColumn = Some(Config.PartitionColumn.EventName)
      ),
      bad = Config.SinkWithMaxSize(
        sink = PubsubSinkConfigM[Id](
          topic                = PubsubSinkConfig.Topic("myproject", "snowplow-bad"),
          batchSize            = 1000,
          requestByteThreshold = 1000000,
          retries              = PubsubSinkConfig.Retries(PubsubSinkConfig.TransientErrorRetrying(delay = 100.millis, attempts = 10))
        ),
        maxRecordSize = 9000000
      )
    ),
    inMemBatchBytes        = 50000000,
    cpuParallelismFraction = BigDecimal("0.75"),
    numEagerWindows        = 1,
    windowing              = 5.minutes,
    spark = Config.Spark(
      taskRetries        = 3,
      writerPartitioning = Config.WriterPartitioning(splitsPerFairShare = 4, minEventsPerSplit = 5000),
      conf               = defaultSparkConf
    ),
    telemetry = Telemetry.Config(
      disable         = false,
      collectorUri    = uri"https://collector-g.snowplowanalytics.com",
      userProvidedId  = None,
      autoGeneratedId = None,
      instanceId      = None,
      moduleName      = None,
      moduleVersion   = None
    ),
    monitoring = Config.Monitoring(
      metrics     = Config.Metrics(None, CommonMetrics.PrometheusConfig(Map.empty)),
      sentry      = None,
      healthProbe = Config.HealthProbe(port = Port.fromInt(8000).get, unhealthyLatency = 1.minute),
      webhook     = Webhook.Config(endpoint = None, tags = Map.empty, heartbeat = 5.minutes)
    ),
    license                 = AcceptedLicense(),
    skipSchemas             = List.empty,
    respectIgluNullability  = true,
    exitOnMissingIgluSchema = true,
    entitySupersetCapacity  = 500,
    retries = Config.Retries(
      setupErrors     = Retrying.Config.ForSetup(delay = 30.seconds),
      transientErrors = Retrying.Config.ForTransient(delay = 1.second, attempts = 5)
    ),
    http          = Config.Http(HttpClient.Config(4)),
    decompression = DecompressionConfig(maxBytesInBatch = 5242880, maxBytesSinglePayload = 10000000)
  )

  private val referenceConfig = Config[PubsubFactoryConfig, PubsubSourceConfig, PubsubSinkConfig](
    streams = PubsubFactoryConfig(emulatorHost = None),
    input = PubsubSourceConfig(
      subscription            = PubsubSourceConfig.Subscription("myproject", "snowplow-enriched"),
      parallelPullFactor      = BigDecimal("2.0"),
      durationPerAckExtension = 10.minutes,
      minRemainingAckDeadline = BigDecimal("0.1"),
      maxMessagesPerPull      = 1000,
      debounceRequests        = 100.millis,
      streamingPull           = false,
      retries                 = PubsubSourceConfig.Retries(PubsubSourceConfig.TransientErrorRetrying(delay = 100.millis, attempts = 10))
    ),
    output = Config.Output(
      good = Config.Delta(
        location = URI.create("gs://my-bucket/events"),
        deltaTableProperties = Map(
          "delta.logRetentionDuration" -> "interval 1 days",
          "delta.deletedFileRetentionDuration" -> "interval 1 days",
          "delta.dataSkippingStatsColumns" -> "load_tstamp,collector_tstamp,derived_tstamp,dvce_created_tstamp,true_tstamp,event_name,app_id",
          "delta.checkpointInterval" -> "50"
        ),
        partitionColumn = Some(Config.PartitionColumn.EventName)
      ),
      bad = Config.SinkWithMaxSize(
        sink = PubsubSinkConfigM[Id](
          topic                = PubsubSinkConfig.Topic("myproject", "snowplow-bad"),
          batchSize            = 100,
          requestByteThreshold = 1000000,
          retries              = PubsubSinkConfig.Retries(PubsubSinkConfig.TransientErrorRetrying(delay = 100.millis, attempts = 10))
        ),
        maxRecordSize = 9000000
      )
    ),
    inMemBatchBytes        = 25600000,
    cpuParallelismFraction = BigDecimal("0.75"),
    numEagerWindows        = 1,
    windowing              = 5.minutes,
    spark = Config.Spark(
      taskRetries        = 3,
      writerPartitioning = Config.WriterPartitioning(splitsPerFairShare = 4, minEventsPerSplit = 5000),
      conf = defaultSparkConf ++ Map(
        "spark.ui.enabled" -> "true"
      )
    ),
    telemetry = Telemetry.Config(
      disable         = false,
      collectorUri    = uri"https://collector-g.snowplowanalytics.com",
      userProvidedId  = Some("my_company"),
      autoGeneratedId = None,
      instanceId      = None,
      moduleName      = None,
      moduleVersion   = None
    ),
    monitoring = Config.Monitoring(
      metrics = Config.Metrics(
        statsd = Some(
          CommonMetrics.StatsdConfig(
            hostname = "127.0.0.1",
            port     = 8125,
            tags     = Map("myTag" -> "xyz"),
            period   = 1.minute,
            prefix   = "snowplow.lakeloader"
          )
        ),
        prometheus = CommonMetrics.PrometheusConfig(Map("myTag" -> "xyz"))
      ),
      sentry = Some(Sentry.ConfigM[Id](dsn = "https://public@sentry.example.com/1", environment = None, tags = Map("myTag" -> "xyz"))),
      healthProbe = Config.HealthProbe(
        port             = Port.fromInt(8000).get,
        unhealthyLatency = 15.minutes
      ),
      webhook =
        Webhook.Config(endpoint = Some(uri"https://webhook.acme.com"), tags = Map("pipeline" -> "production"), heartbeat = 60.minutes)
    ),
    license = AcceptedLicense(),
    skipSchemas = List(
      SchemaCriterion.parse("iglu:com.acme/skipped1/jsonschema/1-0-0").get,
      SchemaCriterion.parse("iglu:com.acme/skipped2/jsonschema/1-0-*").get,
      SchemaCriterion.parse("iglu:com.acme/skipped3/jsonschema/1-*-*").get,
      SchemaCriterion.parse("iglu:com.acme/skipped4/jsonschema/*-*-*").get
    ),
    respectIgluNullability  = true,
    exitOnMissingIgluSchema = true,
    entitySupersetCapacity  = 500,
    retries = Config.Retries(
      setupErrors     = Retrying.Config.ForSetup(delay = 30.seconds),
      transientErrors = Retrying.Config.ForTransient(delay = 1.second, attempts = 5)
    ),
    http          = Config.Http(HttpClient.Config(4)),
    decompression = DecompressionConfig(maxBytesInBatch = 5242880, maxBytesSinglePayload = 10000000)
  )
}
