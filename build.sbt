/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

lazy val root = project
  .in(file("."))
  .aggregate(
    core,
    azure,
    gcp,
    aws
  )

lazy val core: Project = project
  .in(file("modules/core"))
  .settings(BuildSettings.commonSettings)
  .settings(BuildSettings.igluTestSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)

lazy val azure: Project = project
  .in(file("modules/azure"))
  .settings(BuildSettings.azureSettings)
  .settings(libraryDependencies ++= Dependencies.azureDependencies ++ Dependencies.icebergDeltaRuntimeDependencies)
  .dependsOn(core, deltaIceberg)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val gcp: Project = project
  .in(file("modules/gcp"))
  .settings(BuildSettings.gcpSettings)
  .settings(libraryDependencies ++= Dependencies.gcpDependencies ++ Dependencies.icebergDeltaRuntimeDependencies)
  .dependsOn(core, deltaIceberg)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val aws: Project = project
  .in(file("modules/aws"))
  .settings(BuildSettings.awsSettings)
  .settings(libraryDependencies ++= Dependencies.awsDependencies ++ Dependencies.icebergDeltaRuntimeDependencies)
  .dependsOn(core, deltaIceberg)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

/** Packaging: Extra runtime dependencies for alternative assets */

lazy val deltaIceberg: Project = project
  .in(file("packaging/delta-iceberg"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.icebergDeltaRuntimeDependencies)

ThisBuild / fork := true
