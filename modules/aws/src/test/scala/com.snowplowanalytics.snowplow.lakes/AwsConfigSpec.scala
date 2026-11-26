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
import cats.effect.{ExitCode, IO, Resource}
import com.comcast.ip4s.Port
import org.http4s.implicits.http4sLiteralsSyntax
import org.specs2.Specification

import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, ConfigParser, HttpClient, Retrying, Sentry, Telemetry, Webhook}
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig
import com.snowplowanalytics.snowplow.streams.kinesis.{
  BackoffPolicy,
  KinesisHttpSourceConfig,
  KinesisSinkConfig,
  KinesisSinkConfigM,
  KinesisSourceConfig
}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.concurrent.duration.DurationInt

class AwsConfigSpec extends Specification with CatsEffect {

  def is = s2"""
   Config parse should be able to parse
    minimal aws config $minimal
    reference aws config $reference

   partitionColumn, with reference.conf merged in
    Delta: an unquoted null is no partitioning $deltaNull
    Delta: a missing key is event_name $deltaMissing
    Delta: app_id is app_id $deltaAppId
    Iceberg: an unquoted null fails $icebergNull
    Delta: the string "null" fails $deltaQuotedNull
    Iceberg: the string "null" fails $icebergQuotedNull
  """

  private def deltaNull =
    parseGood(deltaGood("partitionColumn: null")).map(_.map(_.tablePartitionColumn) must beRight(beNone))

  private def deltaMissing =
    parseGood(deltaGood("")).map(
      _.map(_.tablePartitionColumn) must beRight(beSome(Config.PartitionColumn.EventName: Config.PartitionColumn))
    )

  private def deltaAppId =
    parseGood(deltaGood("partitionColumn: app_id")).map(
      _.map(_.tablePartitionColumn) must beRight(beSome(Config.PartitionColumn.AppId: Config.PartitionColumn))
    )

  private def icebergNull =
    parseGood(icebergGood("partitionColumn: null")).map(_ must beLeft)

  private def deltaQuotedNull =
    parseGood(deltaGood("partitionColumn: \"null\"")).map(_ must beLeft)

  private def icebergQuotedNull =
    parseGood(icebergGood("partitionColumn: \"null\"")).map(_ must beLeft)

  private def deltaGood(partitionColumn: String): String =
    s"""{ "location": "s3://my-bucket/events", $partitionColumn }"""

  private def icebergGood(partitionColumn: String): String =
    s"""{ "type": "Iceberg", "database": "snowplow", "table": "events", "catalog": { "type": "Glue" }, $partitionColumn }"""

  /** The minimal config with the given `output.good`, through the same parser the loader uses */
  private def parseGood(good: String): IO[Either[String, Config.Target]] = {
    val hocon =
      s"""{
         |  "license": { "accept": true }
         |  "input": { "streamName": "sp-enriched" }
         |  "output": { "good": $good, "bad": { "streamName": "bad" } }
         |}""".stripMargin
    Resource
      .make(IO.blocking(Files.createTempFile("partition-by", ".hocon")))(p => IO.blocking(Files.delete(p)))
      .use { path =>
        IO.blocking(Files.write(path, hocon.getBytes(StandardCharsets.UTF_8))) *>
          ConfigParser
            .configFromFile[IO, Config[Unit, KinesisHttpSourceConfig, KinesisSinkConfig]](path)
            .value
            .map(_.map(_.output.good))
      }
  }

  private def minimal =
    assert(
      resource = "/config.aws.minimal.hocon",
      expectedResult = Right(
        AwsConfigSpec.minimalConfig
      )
    )

  private def reference =
    assert(
      resource = "/config.aws.reference.hocon",
      expectedResult = Right(
        AwsConfigSpec.referenceConfig
      )
    )

  private def assert(resource: String, expectedResult: Either[ExitCode, Config[Unit, KinesisHttpSourceConfig, KinesisSinkConfig]]) = {
    val path = Paths.get(getClass.getResource(resource).toURI)
    ConfigParser.configFromFile[IO, Config[Unit, KinesisHttpSourceConfig, KinesisSinkConfig]](path).value.map { result =>
      result must beEqualTo(expectedResult)
    }
  }
}

// workerIdentifier comes from the HOSTNAME env variable set in BuildSettings
object AwsConfigSpec {

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

  private val minimalConfig = Config[Unit, KinesisHttpSourceConfig, KinesisSinkConfig](
    streams = (),
    input = KinesisHttpSourceConfig(
      KinesisSourceConfig(
        appName                          = "snowplow-lake-loader",
        streamName                       = "sp-enriched",
        workerIdentifier                 = "test-hostname",
        initialPosition                  = KinesisSourceConfig.InitialPosition.Latest,
        retrievalMode                    = KinesisSourceConfig.Retrieval.Polling(750, 1500.millis),
        customEndpoint                   = None,
        dynamodbCustomEndpoint           = None,
        cloudwatchCustomEndpoint         = None,
        leaseDuration                    = 10.seconds,
        maxLeasesToStealAtOneTimeFactor  = BigDecimal("2.0"),
        checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
        debounceCheckpoints              = 10.seconds,
        maxRetries                       = 10,
        apiCallAttemptTimeout            = 15.seconds
      ),
      None
    ),
    output = Config.Output(
      good = Config.Delta(
        location = URI.create("s3://my-bucket/events"),
        deltaTableProperties = Map(
          "delta.logRetentionDuration" -> "interval 1 days",
          "delta.deletedFileRetentionDuration" -> "interval 1 days",
          "delta.dataSkippingStatsColumns" -> "load_tstamp,collector_tstamp,derived_tstamp,dvce_created_tstamp,true_tstamp,event_name,app_id",
          "delta.checkpointInterval" -> "50"
        ),
        partitionColumn = Some(Config.PartitionColumn.EventName)
      ),
      bad = Config.SinkWithMaxSize(
        sink = KinesisSinkConfigM[Id](
          streamName             = "bad",
          throttledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
          recordLimit            = 500,
          byteLimit              = 5242880,
          customEndpoint         = None,
          maxRetries             = 10
        ),
        maxRecordSize = 1000000
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

  private val referenceConfig = Config[Unit, KinesisHttpSourceConfig, KinesisSinkConfig](
    streams = (),
    input = KinesisHttpSourceConfig(
      KinesisSourceConfig(
        appName                          = "snowplow-lake-loader",
        streamName                       = "snowplow-enriched-events",
        workerIdentifier                 = "test-hostname",
        initialPosition                  = KinesisSourceConfig.InitialPosition.TrimHorizon,
        retrievalMode                    = KinesisSourceConfig.Retrieval.Polling(1000, 1500.millis),
        customEndpoint                   = None,
        dynamodbCustomEndpoint           = None,
        cloudwatchCustomEndpoint         = None,
        leaseDuration                    = 10.seconds,
        maxLeasesToStealAtOneTimeFactor  = BigDecimal("2.0"),
        checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
        debounceCheckpoints              = 10.seconds,
        maxRetries                       = 10,
        apiCallAttemptTimeout            = 15.seconds
      ),
      None
    ),
    output = Config.Output(
      good = Config.Delta(
        location = URI.create("s3a://my-bucket/events"),
        deltaTableProperties = Map(
          "delta.logRetentionDuration" -> "interval 1 days",
          "delta.deletedFileRetentionDuration" -> "interval 1 days",
          "delta.dataSkippingStatsColumns" -> "load_tstamp,collector_tstamp,derived_tstamp,dvce_created_tstamp,true_tstamp,event_name,app_id",
          "delta.checkpointInterval" -> "50"
        ),
        partitionColumn = Some(Config.PartitionColumn.EventName)
      ),
      bad = Config.SinkWithMaxSize(
        sink = KinesisSinkConfigM[Id](
          streamName             = "bad",
          throttledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
          recordLimit            = 500,
          byteLimit              = 5242880,
          customEndpoint         = None,
          maxRetries             = 10
        ),
        maxRecordSize = 1000000
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
        "spark.ui.enabled" -> "true",
        "fs.s3a.aws.credentials.provider" -> "com.amazonaws.auth.InstanceProfileCredentialsProvider"
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
