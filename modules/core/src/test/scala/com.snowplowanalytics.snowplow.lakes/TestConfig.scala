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

import com.typesafe.config.ConfigFactory
import io.circe.config.syntax._
import io.circe.Json

import fs2.io.file.Path

object TestConfig {

  sealed trait Target
  case object Delta extends Target
  case object Hudi extends Target
  case object Iceberg extends Target

  /** Provides an app Config using defaults provided by our standard reference.conf */
  def defaults(target: Target, tmpDir: Path): AnyConfig =
    ConfigFactory
      .load(ConfigFactory.parseString(configOverrides(target, tmpDir)))
      .as[Config[Option[Unit], Json, Json]] match {
      case Right(ok) => ok
      case Left(e)   => throw new RuntimeException("Could not load default config for testing", e)
    }

  private def configOverrides(target: TestConfig.Target, tmpDir: Path): String = {
    val location = (tmpDir / "events").toNioPath.toUri
    target match {
      case Delta =>
        s"""
        $commonRequiredConfig
        output.good: {
          type: "Delta"
          location: "$location"
        }
        """
      case Hudi =>
        s"""
        $commonRequiredConfig
        output.good: {
          type: "Hudi"
          location: "$location"
        }
        """
      case Iceberg =>
        s"""
        $commonRequiredConfig
        output.good: {
          type: "Iceberg"
          database: "test"
          table: "events"
          location: "${tmpDir.toNioPath.toUri}"
          catalog: {
            type: Hadoop
          }
        }
        """
    }
  }

  private def commonRequiredConfig: String =
    """
    license: {
      accept: true
    }
    streams: {}
    input: {}
    output.bad: {
      maxRecordSize: 10000
    }
    """

}
