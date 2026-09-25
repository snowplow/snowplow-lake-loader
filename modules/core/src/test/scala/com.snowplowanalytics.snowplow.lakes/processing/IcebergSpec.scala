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
import cats.effect.kernel.Resource
import cats.implicits._
import org.specs2.matcher.MatchResult

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.snowplow.lakes.{Config, TestConfig, TestSparkEnvironment}
import com.snowplowanalytics.snowplow.lakes.tables.IcebergWriter

import fs2.io.file.{Files, Path}

class IcebergSpec extends AbstractSparkSpec {

  import AbstractSparkSpec._

  override def is = super.is ^ s2"""
    Report a table_snapshots_retained matching the .snapshots metadata table, without submitting a Spark job $eSnapshotsRetained
  """

  override def target: TestConfig.Target = TestConfig.Iceberg

  override def supportsRequiredNestedFields: Boolean = true

  /** Reads the table back into memory, so we can make assertions on the app's output */
  override def readTable(spark: SparkSession, tmpDir: Path): DataFrame =
    spark.sql("select * from test_catalog.test.events")

  /**
   * Spark config used only while reading table back into memory for assertions.
   *
   * Registers the loader's own catalog alongside `test_catalog`, so that the snapshots example can
   * call `IcebergWriter` against this session rather than opening a second one. Both point at the
   * same Hadoop warehouse.
   */
  override def sparkConfig(tmpDir: Path): Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.test_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.test_catalog.type" -> "hadoop",
      "spark.sql.catalog.test_catalog.warehouse" -> tmpDir.toString
    ) ++ new IcebergWriter(icebergConfig(tmpDir)).sparkConfig

  /**
   * `getTableSnapshotsRetained` reads `Table.snapshots()` rather than counting the `.snapshots`
   * metadata table, which is the Spark job it exists to avoid. `SnapshotsTable` builds that table's
   * rows from the same call, so the two have to agree - this pins that they do, and that reaching
   * the answer still costs no job.
   */
  def eSnapshotsRetained = Files[IO].tempDirectory.use { tmpDir =>
    val resources = for {
      inputs <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good()))
      tokened <- Resource.eval(inputs.traverse(_.tokened))
      env <- TestSparkEnvironment.build(target, tmpDir, List(tokened))
    } yield env

    val writer = new IcebergWriter(icebergConfig(tmpDir))

    resources.use(env => Processing.stream(env).compile.drain) *> {
      sparkForAssertions(sparkConfig(tmpDir)).use { spark =>
        for {
          counted <- countingSparkJobs(spark)(writer.getTableSnapshotsRetained[IO](spark))
          (retained, jobs) = counted
          viaMetadataTable <- IO.blocking(spark.table("iceberg_catalog.`test`.`events`.snapshots").count())
        } yield List[MatchResult[Any]](
          retained must beSome(viaMetadataTable),
          // The empty commit made by `prepareTable`, then the window
          retained must beSome(2L),
          jobs must beEqualTo(0)
        ).reduce(_ and _)
      }
    }
  }

  /**
   * Runs `f` and reports how many Spark jobs it submitted.
   *
   * A listener is notified asynchronously, so the count is read behind a sentinel job submitted
   * after `f` returns: events reach a listener in the order they were posted, so once the
   * sentinel's own start has arrived, every job `f` submitted has already been counted.
   */
  private def countingSparkJobs[A](spark: SparkSession)(f: IO[A]): IO[(A, Int)] = {
    val sentinel = s"sentinel-${java.util.UUID.randomUUID}"
    val jobs     = new AtomicInteger(0)
    val seen     = new AtomicBoolean(false)

    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit =
        if (Option(jobStart.properties).map(_.getProperty("spark.job.description")).contains(sentinel))
          seen.set(true)
        else {
          val _ = jobs.incrementAndGet()
        }
    }

    def awaitSentinel: IO[Unit] =
      IO.delay(seen.get).flatMap {
        case true  => IO.unit
        case false => IO.sleep(10.millis) *> awaitSentinel
      }

    val register = IO.blocking(spark.sparkContext.addSparkListener(listener))
    val remove   = IO.blocking(spark.sparkContext.removeSparkListener(listener))

    Resource.make(register)(_ => remove).use { _ =>
      for {
        a <- f
        _ <- IO.blocking {
               spark.sparkContext.setJobDescription(sentinel)
               try {
                 val _ = spark.sparkContext.parallelize(Seq(1), 1).count()
               } finally spark.sparkContext.setJobDescription(null)
             }
        _ <- awaitSentinel.timeout(30.seconds)
      } yield (a, jobs.get)
    }
  }

  private def icebergConfig(tmpDir: Path): Config.Iceberg =
    TestConfig.defaults(target, tmpDir).output.good match {
      case iceberg: Config.Iceberg => iceberg
      case other                   => throw new IllegalStateException(s"Expected an Iceberg target, got $other")
    }
}
