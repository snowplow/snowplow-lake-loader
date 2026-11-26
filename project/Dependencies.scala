/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
import sbt._

object Dependencies {

  object V {
    // Snowplow libraries
    val streams    = "0.29.0"
    val igluClient = "4.3.0"

    // Scala and FP core
    val decline            = "2.4.1"
    val circeGenericExtras = "0.14.4" // Released separately from circe

    // Spark and the table formats
    object Spark {

      // A version of Spark which is compatible with the current version of Iceberg and Delta
      val forIcebergDelta      = "4.1.4"
      val forIcebergDeltaMinor = "4.1"
    }
    val delta   = "4.4.1"
    val iceberg = "1.12.0"
    val lz4     = "1.11.4"

    // Hadoop, and the cloud SDKs behind the storage clients
    val hadoop             = "3.5.0"
    val awsSdk2            = "2.54.18" // Aligned with common-streams
    val awsSdk1            = "1.12.797"
    val azureSdk           = "1.18.6"
    val azureDatalake      = "12.26.3"
    val googleCloudStorage = "2.72.0"

    // Logging
    val slf4j = "2.0.18"

    // Versions held above what something upstream asks for, to keep a vulnerable copy out of the
    // images. See securityOverrides below for where each one applies.
    val log4j        = "2.25.5"
    val jackson      = "2.22.4"
    val ivy          = "2.6.0"
    val snappy       = "1.1.10.9"
    val bouncyCastle = "1.86"
    val netty        = "4.2.18.Final"
    val reactorNetty = "1.3.7"
    val guava        = "33.7.2-jre"
    val micrometer   = "1.16.7"
    val compressLzf  = "1.2.0"

    // Held above upstream for binary compatibility rather than security: netty-tcnative has its own
    // version scheme, and must match the tcnative.version pinned by the netty-parent pom of V.netty,
    // because netty-handler calls into it via JNI-bound statics
    val nettyTcnative = "2.0.84.Final"

    // Test only
    val catsEffect       = "3.7.1"
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"

    // Compiler plugins
    val betterMonadicFor = "0.3.1"
  }

  // Snowplow libraries
  val streamsCore      = "com.snowplowanalytics" %% "streams-core"             % V.streams
  val kinesis          = "com.snowplowanalytics" %% "kinesis"                  % V.streams
  val kafka            = "com.snowplowanalytics" %% "kafka"                    % V.streams
  val pubsub           = "com.snowplowanalytics" %% "pubsub"                   % V.streams
  val loaders          = "com.snowplowanalytics" %% "loaders-common"           % V.streams
  val runtime          = "com.snowplowanalytics" %% "runtime-common"           % V.streams
  val igluClientHttp4s = "com.snowplowanalytics" %% "iglu-scala-client-http4s" % V.igluClient

  // Scala and FP core
  val decline           = "com.monovore" %% "decline-effect"       % V.decline
  val circeGenericExtra = "io.circe"     %% "circe-generic-extras" % V.circeGenericExtras

  // Spark and the table formats
  object Spark {
    val coreForIcebergDelta = "org.apache.spark" %% "spark-core" % V.Spark.forIcebergDelta
    val sqlForIcebergDelta  = "org.apache.spark" %% "spark-sql"  % V.Spark.forIcebergDelta
  }
  val delta   = "io.delta"           %% s"delta-spark_${V.Spark.forIcebergDeltaMinor}"           % V.delta
  val iceberg = "org.apache.iceberg" %% s"iceberg-spark-runtime-${V.Spark.forIcebergDeltaMinor}" % V.iceberg
  // Stands in for org.lz4:lz4-java, which commonExclusions removes; Spark needs the classes
  val lz4 = "at.yawk.lz4" % "lz4-java" % V.lz4

  // Hadoop, and the cloud SDKs behind the storage clients
  val hadoopClient  = "org.apache.hadoop"      % "hadoop-client-runtime"     % V.hadoop
  val hadoopAzure   = "org.apache.hadoop"      % "hadoop-azure"              % V.hadoop
  val hadoopAws     = "org.apache.hadoop"      % "hadoop-aws"                % V.hadoop
  val hadoopGcp     = "org.apache.hadoop"      % "hadoop-gcp"                % V.hadoop
  val deltaDynamodb = "io.delta"               % "delta-storage-s3-dynamodb" % V.delta
  val awsGlue       = "software.amazon.awssdk" % "glue"                      % V.awsSdk2
  val awsS3         = "software.amazon.awssdk" % "s3"                        % V.awsSdk2
  val awsS3Transfer = "software.amazon.awssdk" % "s3-transfer-manager"       % V.awsSdk2
  val awsSts        = "software.amazon.awssdk" % "sts"                       % V.awsSdk2
  val awsKms        = "software.amazon.awssdk" % "kms"                       % V.awsSdk2
  // S3A in hadoop-aws builds the AWS SDK's HttpClient 4 client, which it would otherwise get from
  // the SDK bundle excluded from hadoopAws below. The SDK's own poms ship only the HttpClient 5
  // client at runtime, which is the one Iceberg's S3, Glue and KMS clients use.
  val awsApacheClient = "software.amazon.awssdk" % "apache-client"               % V.awsSdk2
  val dynamodbSdk1    = "com.amazonaws"          % "aws-java-sdk-dynamodb"       % V.awsSdk1
  val azureIdentity   = "com.azure"              % "azure-identity"              % V.azureSdk
  val azureDatalake   = "com.azure"              % "azure-storage-file-datalake" % V.azureDatalake
  // The client behind Iceberg's GCSFileIO, and the plain exception classes DestinationSetupError
  // matches on that path
  val googleCloudStorage = "com.google.cloud" % "google-cloud-storage" % V.googleCloudStorage

  // Logging
  val slf4j = "org.slf4j" % "slf4j-simple" % V.slf4j

  // Artifacts named only to force a version up; grouped into securityOverrides below
  val log4jTemplateJson  = "org.apache.logging.log4j"         % "log4j-layout-template-json" % V.log4j
  val log4j12Api         = "org.apache.logging.log4j"         % "log4j-1.2-api"              % V.log4j
  val jacksonModuleScala = "com.fasterxml.jackson.module"    %% "jackson-module-scala"       % V.jackson
  val ivy                = "org.apache.ivy"                   % "ivy"                        % V.ivy
  val snappy             = "org.xerial.snappy"                % "snappy-java"                % V.snappy
  val bouncyCastle       = "org.bouncycastle"                 % "bcprov-jdk18on"             % V.bouncyCastle
  val reactorNettyHttp   = "io.projectreactor.netty"          % "reactor-netty-http"         % V.reactorNetty
  val guava              = "com.google.guava"                 % "guava"                      % V.guava
  val micrometer         = "io.micrometer"                    % "micrometer-core"            % V.micrometer
  val jacksonCbor        = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"    % V.jackson
  val compressLzf        = "com.ning"                         % "compress-lzf"               % V.compressLzf
  // Netty modules must share a version. Naming these pulls the remaining modules up alongside them.
  // netty-all is deliberately left out: it is a classless aggregator whose non-optional dependencies
  // would drag in unused protocol modules (mqtt, redis, rxtx, ...) if declared directly.
  val nettyModules: Seq[ModuleID] = Seq(
    "netty-codec",
    "netty-codec-http2",
    "netty-codec-http3",
    "netty-codec-native-quic",
    "netty-codec-protobuf",
    "netty-handler-proxy",
    "netty-resolver-dns",
    "netty-transport-native-epoll",
    "netty-transport-native-io_uring",
    "netty-transport-native-kqueue"
  ).map("io.netty" % _ % V.netty)

  // Named to force a version up for binary compatibility rather than security
  val nettyTcnative = "io.netty" % "netty-tcnative-boringssl-static" % V.nettyTcnative

  // Test only
  val specs2            = "org.specs2"    %% "specs2-core"                % V.specs2           % Test
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect       % Test
  val catsEffectSpecs2  = "org.typelevel" %% "cats-effect-testing-specs2" % V.catsEffectSpecs2 % Test

  // Compiler plugins
  val betterMonadicFor = "com.olegpy" %% "better-monadic-for" % V.betterMonadicFor

  // Security overrides. An entry is needed only while it still evicts something: once the upstream
  // catches up, delete it. To check, drop the line and compare the resolved runtime classpath
  // before and after; reading the dependency tree for eviction markers is unreliable, because sbt
  // truncates long lines. They are all declared on core, which every app module depends on.
  //
  // Spark brings in log4j, jackson-module-scala, ivy, snappy-java, and netty-all, whose
  // non-optional dependency on bcprov-jdk18on is the only path for BouncyCastle. Naming the two log4j
  // modules pulls log4j-core and log4j-api up alongside; naming jackson-module-scala does the same
  // for jackson-databind and jackson-core.
  // Netty also arrives via the Azure SDK, reactor-netty-http and the AWS SDK. tcnative is not itself
  // vulnerable, but has to move in step with netty, so it goes whenever the netty override goes.
  // The Azure SDK brings in reactor-netty-http, and naming it pulls reactor-core up alongside.
  // google-cloud-storage brings in guava, and runtime-common brings in micrometer-core.
  val securityOverrides: Seq[ModuleID] = Seq(
    log4jTemplateJson,
    log4j12Api,
    jacksonModuleScala,
    ivy,
    snappy,
    bouncyCastle,
    reactorNettyHttp,
    guava,
    micrometer % Runtime,
    jacksonCbor,
    compressLzf,
    nettyTcnative
  ) ++ nettyModules

  val commonRuntimeDependencies = Seq(
    slf4j % Runtime
  )

  // Every image can write to every store, whichever cloud it is named for
  val storageDependencies = Seq(
    // S3
    hadoopAws.exclude("software.amazon.awssdk", "bundle"),
    awsS3,
    awsGlue,
    awsSts,
    dynamodbSdk1,
    awsKms          % Runtime,
    awsApacheClient % Runtime,
    deltaDynamodb   % Runtime,
    awsS3Transfer   % Runtime,
    // ABFS
    azureIdentity,
    azureDatalake,
    hadoopAzure,
    // GCS
    hadoopGcp,
    googleCloudStorage
  )

  val coreDependencies = Seq(
    streamsCore,
    loaders,
    runtime,
    delta,
    Spark.coreForIcebergDelta,
    Spark.sqlForIcebergDelta,
    iceberg,
    igluClientHttp4s,
    decline,
    circeGenericExtra,
    hadoopClient,
    lz4,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    slf4j % Test
  ) ++ commonRuntimeDependencies ++ storageDependencies ++ securityOverrides

  val awsDependencies = Seq(
    kinesis,
    specs2,
    catsEffectSpecs2
  ) ++ commonRuntimeDependencies

  val azureDependencies = Seq(
    kafka,
    specs2,
    catsEffectSpecs2
  ) ++ commonRuntimeDependencies

  val gcpDependencies = Seq(
    pubsub,
    specs2,
    catsEffectSpecs2
  ) ++ commonRuntimeDependencies

  val commonExclusions = Seq(
    ExclusionRule(organization = "org.apache.zookeeper", name     = "zookeeper"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-client"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-server"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-http"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-webapp"),
    ExclusionRule(organization = "org.apache.kerby"),
    ExclusionRule(organization = "org.apache.hadoop", name        = "hadoop-yarn-server-applicationhistoryservice"),
    ExclusionRule(organization = "org.apache.hadoop", name        = "hadoop-yarn-server-common"),
    ExclusionRule(organization = "com.github.joshelser", name     = "dropwizard-metrics-hadoop-metrics2-reporter"),
    ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-slf4j2-impl"),
    ExclusionRule(organization = "org.lz4", name                  = "lz4-java") // replaced by at.yawk.lz4:lz4-java
  )

}
