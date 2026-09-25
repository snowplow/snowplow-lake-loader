/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.processing

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import org.apache.spark.sql.delta.DeltaLog
import org.specs2.Specification

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

import com.snowplowanalytics.snowplow.lakes.{Config, TestConfig}
import com.snowplowanalytics.snowplow.lakes.tables.DeltaWriter

import scala.concurrent.duration.DurationInt

// Uses SparkUtils (private[processing]), so this test must live in the same package.
class DeltaWriterSpec extends Specification with CatsEffect {

  override val Timeout = 60.seconds

  def is = sequential ^ s2"""
  DeltaWriter.prepareTable should:
    Open a pre-existing table whose properties lack the current defaults $e1

  DeltaWriter.describeTable should:
    Report the properties of the existing table, not the ones this loader is configured with $e2
    Report the features a legacy protocol implies rather than none $e3
  """

  /**
   * Guards the upgrade path for tables created by older loaders, whose table properties lack
   * delta.deletedFileRetentionDuration.
   *
   * When such a table's schema exactly matches the current atomic schema (it never received a
   * self-describing event or context), Delta's createIfNotExists reaches its property comparison
   * and throws DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY, which prepareTable must tolerate: an
   * existing table keeps its properties. (Tables with evolved schemas never reach that comparison
   * because the schema check throws DELTA_CREATE_TABLE_SCHEME_MISMATCH first, which prepareTable
   * already tolerates.)
   */
  def e1 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)
    val delta = config.output.good match {
      case d: Config.Delta => d
      case other           => throw new IllegalStateException(s"Expected a Delta target but got $other")
    }
    SparkUtils.session[IO](config.spark, new DeltaWriter(delta), delta, cores = 2).use { spark =>
      for {
        _ <- new DeltaWriter(delta).prepareTable[IO](spark)
        // Rewrite commit 0 into the shape an old loader left behind: no deletedFileRetentionDuration.
        // (Delta 4.3 cannot create that state directly: a fresh CREATE with logRetentionDuration=1d
        // and no deletedFileRetentionDuration fails its retention compatibility check.)
        _ <- IO.blocking {
               val logDir     = Paths.get(java.net.URI.create(delta.location.toString).getPath, "_delta_log")
               val commitZero = logDir.resolve("00000000000000000000.json")
               val rewritten =
                 Files.readString(commitZero).replace("\"delta.deletedFileRetentionDuration\":\"interval 1 days\",", "")
               Files.writeString(commitZero, rewritten)
               Files.list(logDir).iterator.asScala.filter(_.toString.endsWith(".crc")).foreach(Files.delete)
               DeltaLog.clearCache()
             }
        // A restarted loader has an empty in-memory session catalog, so createIfNotExists cannot
        // take its ignore-if-exists early exit on the catalog entry; reproduce that state
        _ <- IO.blocking(spark.sql("DROP TABLE IF EXISTS default.events_internal_id"): Unit)
        result <- new DeltaWriter(delta).prepareTable[IO](spark).attempt
      } yield result must beRight
    }
  }

  /**
   * A restarted loader's configured properties are not the table's - see `Writer.describeTable`.
   *
   * The second writer runs `prepareTable` first, so what is described is the state a restart leaves
   * behind rather than a table the writer never touched. Its DROP TABLE reproduces the empty
   * in-memory session catalog of a restart, without which createIfNotExists exits early on the
   * catalog entry and never compares properties.
   */
  def e2 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)
    val delta = config.output.good match {
      case d: Config.Delta => d
      case other           => throw new IllegalStateException(s"Expected a Delta target but got $other")
    }
    def withCheckpointInterval(n: String): Config.Delta =
      delta.copy(deltaTableProperties = delta.deltaTableProperties + ("delta.checkpointInterval" -> n))

    val asCreated   = new DeltaWriter(withCheckpointInterval("42"))
    val asRestarted = new DeltaWriter(withCheckpointInterval("7"))

    SparkUtils.session[IO](config.spark, asCreated, delta, cores = 2).use { spark =>
      for {
        _ <- asCreated.prepareTable[IO](spark)
        _ <- IO.blocking(spark.sql("DROP TABLE IF EXISTS default.events_internal_id"): Unit)
        _ <- asRestarted.prepareTable[IO](spark)
        described <- asRestarted.describeTable[IO](spark)
        structure  = described(0)
        properties = described(1)
      } yield List(
        properties must contain("delta.checkpointInterval=\"42\""),
        properties must not contain "delta.checkpointInterval=\"7\"",
        structure must contain("partitioned by [load_tstamp_date, event_name]"),
        structure must contain("min reader version = "),
        structure must contain("min writer version = "),
        // The loader's load_tstamp_date column is generated, so the table always carries that feature
        structure must contain("generatedColumns")
      ).reduce(_ and _)
    }
  }

  /**
   * A legacy protocol names no features, so `e2`'s table - which Delta creates on the
   * table-features protocol - does not reach the branch that reports the ones its version numbers
   * imply.
   */
  def e3 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = TestConfig.defaults(TestConfig.Delta, tmpDir)
    val delta = config.output.good match {
      case d: Config.Delta => d
      case other           => throw new IllegalStateException(s"Expected a Delta target but got $other")
    }
    val legacy =
      delta.copy(deltaTableProperties = delta.deltaTableProperties ++ Map("delta.minReaderVersion" -> "1", "delta.minWriterVersion" -> "4"))
    val writer = new DeltaWriter(legacy)

    SparkUtils.session[IO](config.spark, writer, legacy, cores = 2).use { spark =>
      for {
        _ <- writer.prepareTable[IO](spark)
        described <- writer.describeTable[IO](spark)
        structure = described(0)
      } yield List(
        structure must contain("min writer version = 4"),
        structure must contain("generatedColumns")
      ).reduce(_ and _)
    }
  }
}
