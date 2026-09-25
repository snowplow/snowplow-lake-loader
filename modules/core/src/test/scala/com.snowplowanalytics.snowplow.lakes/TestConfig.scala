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
  case object Iceberg extends Target

  /** Provides an app Config using defaults provided by our standard reference.conf */
  def defaults(
    target: Target,
    tmpDir: Path,
    stageOffHeap: Boolean = false
  ): AnyConfig =
    ConfigFactory
      .load(ConfigFactory.parseString(configOverrides(target, tmpDir, stageOffHeap)))
      .as[Config[Option[Unit], Json, Json]] match {
      case Right(ok) => ok
      case Left(e)   => throw new RuntimeException("Could not load default config for testing", e)
    }

  private def configOverrides(
    target: TestConfig.Target,
    tmpDir: Path,
    stageOffHeap: Boolean
  ): String = {
    val location = (tmpDir / "events").toNioPath.toUri
    target match {
      case Delta =>
        s"""
        ${commonRequiredConfig(stageOffHeap)}
        output.good: {
          type: "Delta"
          location: "$location"
        }
        """
      case Iceberg =>
        s"""
        ${commonRequiredConfig(stageOffHeap)}
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

  private def commonRequiredConfig(stageOffHeap: Boolean): String =
    s"""
    license: {
      accept: true
    }
    streams: {}
    input: {}
    output.bad: {
      maxRecordSize: 10000
    }
    ${if (stageOffHeap) offHeapPool else ""}
    """

  /**
   * A configured pool is what makes `LakeWriter` stage window batches off-heap, so this is how a
   * spec picks that path. Off by default, because reference.conf ships no pool: unless a spec says
   * otherwise it should exercise what customers actually run.
   *
   * Deliberately no `spark.memory.storageFraction` here. reference.conf sets it to "0" and HOCON
   * merges this on top, so specs run with the production split - no guaranteed off-heap storage
   * region, staged blocks borrowing from the execution pool. That they pass is the evidence that
   * borrowing works, which is the non-obvious half of `SparkUtils.stageBatchesOffHeap`.
   */
  private def offHeapPool: String =
    """
    spark.conf: {
      "spark.memory.offHeap.enabled": "true"
      "spark.memory.offHeap.size": "268435456"
    }
    """

}
