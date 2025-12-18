/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

// SBT
import sbt._
import sbt.io.IO
import Keys._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtdynver.DynVerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._

// Iglu plugin
import com.snowplowanalytics.snowplow.sbt.IgluSchemaPlugin.autoImport._

import scala.sys.process._

object BuildSettings {

  // Java 9+ Module System flags needed for both SBT and packaged distributions
  private lazy val javaModuleFlags = Seq(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", // Needed by Kryo serializer
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", // Needed by Kryo serializer
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED", // Needed by Kryo for collections
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", // Critical for Spark StorageUtils
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED" // Needed by Iceberg/Kryo for AtomicBoolean
  )

  lazy val commonSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.13.16",
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false,
    // Target Java 21 bytecode and restrict API to Java 21
    scalacOptions += "-release:21",
    addCompilerPlugin(Dependencies.betterMonadicFor),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-", // to be compatible with docker

    // Java Module System Compatibility
    Compile / javaOptions ++= javaModuleFlags, // For running from SBT (sbt run)

    Compile / resourceGenerators += Def.task {
      val license = (Compile / resourceManaged).value / "META-INF" / "LICENSE"
      IO.copyFile(file("LICENSE.md"), license)
      Seq(license)
    }.taskValue,
    licenses += ("Snowplow Limited Use License Agreement", url("https://docs.snowplow.io/limited-use-license-1.1")),
    headerLicense := Some(
      HeaderLicense.Custom(
        """|Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
          |
          |This software is made available by Snowplow Analytics, Ltd.,
          |under the terms of the Snowplow Limited Use License Agreement, Version 1.1
          |located at https://docs.snowplow.io/limited-use-license-1.1
          |BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
          |OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
          |""".stripMargin
      )
    ),
    headerMappings := headerMappings.value + (HeaderFileType.conf -> HeaderCommentStyle.hashLineComment),
    excludeDependencies ++= Dependencies.commonExclusions,
    libraryDependencySchemes ++= Seq(
      // kafka-clients, spark-core and parquet-hadoop use different versions of zstd-jni.
      // SBT reports version conflict between these versions however this is a Java library and
      // doesn't follow Early Semver version scheme that is recommended for Scala libraries.
      // Therefore, version conflict reports for this library are ignored.
      "com.github.luben" % "zstd-jni" % VersionScheme.Always
    )
  )

  lazy val appSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](dockerAlias, name, version),
    buildInfoPackage := "com.snowplowanalytics.snowplow.lakes",
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo")
  ) ++ commonSettings

  lazy val awsSettings = appSettings ++ Seq(
    name := "lake-loader-aws",
    buildInfoKeys += BuildInfoKey("cloud" -> "AWS"),

    // TODO: Remove this after Hadoop 3.5.0 is released with full support for V2 SDK
    dockerEnvVars += ("AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT" -> "true"),

    // Set Java module flags via JAVA_OPTS environment variable
    dockerEnvVars += ("JAVA_OPTS" -> javaModuleFlags.mkString(" "))
  )

  lazy val azureSettings = appSettings ++ Seq(
    name := "lake-loader-azure",
    buildInfoKeys += BuildInfoKey("cloud" -> "Azure"),

    // Set Java module flags via JAVA_OPTS environment variable
    dockerEnvVars += ("JAVA_OPTS" -> javaModuleFlags.mkString(" "))
  )

  lazy val gcpSettings = appSettings ++ Seq(
    name := "lake-loader-gcp",
    buildInfoKeys += BuildInfoKey("cloud" -> "GCP"),

    // Set Java module flags via JAVA_OPTS environment variable
    dockerEnvVars += ("JAVA_OPTS" -> javaModuleFlags.mkString(" "))
  )

  val igluTestSettings = Seq(
    Test / igluUris := Seq(
      // Iglu Central schemas used in tests will get pre-fetched by sbt
      "iglu:com.snowplowanalytics.snowplow.media/ad_break_end_event/jsonschema/1-0-0"
    )
  )

}
