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

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType, StringType, StructType, TimestampType}
import org.specs2.Specification

import java.io.File
import java.sql.Timestamp
import java.util.UUID

import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.snowplow.lakes.{Config, TestConfig}
import com.snowplowanalytics.snowplow.lakes.tables.DeltaWriter

/**
 * `WriterPartitioner` decides how the window is spread over the writer's partitions, and the whole
 * design assumes the table writer then honours that decision. Iceberg is told to explicitly, via
 * `distribution-mode: none`. Delta is not told anything, so this pins that each writer partition's
 * rows reach the lake as that partition's own files - anything redistributing them between the
 * repartition and the write would undo the packing as a throughput regression rather than a
 * failure.
 *
 * Asserted against the plan `WriterPartitioner` produced rather than as one file per `event_name`,
 * which a redistribution that happened to cluster by `event_name` would also satisfy. A name the
 * packer split across several partitions must arrive as one file per partition; re-clustering it
 * would merge those, so the fixture is chosen to contain such a name.
 *
 * Over the composition `LakeWriter.prepareCommit` and `commit` perform, and through the configured
 * `DeltaWriter`, rather than a bare `format("delta").save` - so the loader's own table properties
 * and write options are part of what is pinned.
 */
class DeltaPartitioningSpec extends Specification with CatsEffect {

  override val Timeout = 120.seconds

  def is = s2"""
  Writing to Delta should:
    Give each event_name as many files as the packer gave it partitions, rather than regroup them $e1
  """

  // One name of 500 events and three of 100, over three writer partitions, with splitting tuned so
  // the big one is cut. Every name must then arrive as exactly as many files as the packer gave it
  // partitions - more means Delta fanned out, fewer means it regrouped the rows.
  def e1 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val viewName = "test_delta_preserves_partitioning_e1"
    val config   = TestConfig.defaults(TestConfig.Delta, tmpDir)
    val delta = config.output.good match {
      case d: Config.Delta => d
      case other           => throw new IllegalStateException(s"Expected a Delta target but got $other")
    }
    val writer        = new DeltaWriter(delta)
    val eventNames    = List.fill(500)("name_hot") ++ (1 to 300).toList.map(i => s"name_cold_${i % 3}")
    val counts        = eventNames.groupBy(name => Option(name)).view.mapValues(_.size).toMap
    val partitioning  = Config.WriterPartitioning(splitsPerFairShare = 2, minEventsPerSplit = 1)
    val plan          = WriterPartitioner.plan(counts, writerParallelism = 3, partitioning)
    val expectedFiles = plan.assignments.map { case (name, partitions) => name.get -> partitions.distinct.size }.toMap

    SparkUtils.session[IO](config.spark, writer, delta, cores = 4).use { spark =>
      val schema = StructType(SparkSchema.atomic)
      val rows   = NonEmptyList.fromListUnsafe(eventNames.map(mkRow(schema, _)))

      for {
        _ <- writer.prepareTable[IO](spark)
        _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
        encoded <- SparkUtils.encodeBatch[IO](spark, rows, schema)
        staged <- SparkUtils.stageBatch[IO](spark, encoded, schema, stageOffHeap = false)
        _ <- SparkUtils.appendStagedBatch[IO](spark, viewName, staged.df, schema, shouldRestoreNullability = false)
        prepared <- SparkUtils.prepareFinalDataFrame[IO](
                      spark,
                      viewName,
                      writerParallelism  = 3,
                      writerPartitioning = partitioning,
                      eventNameCounts    = counts
                    )
        shuffled <- SparkUtils.materializeShuffle[IO](spark, prepared)
        _ <- SparkUtils.replaceView[IO](viewName, shuffled.df)
        toWrite <- SparkUtils.readFinalDataFrame[IO](spark, viewName)
        _ <- writer.write[IO](toWrite)
        filesPerEventName <- IO.blocking {
                               parquetFiles(new File(delta.location))
                                 .groupBy(_.getParentFile.getName.stripPrefix("event_name="))
                                 .view
                                 .mapValues(_.size)
                                 .toMap
                             }
      } yield List(
        // Without a split name this would assert nothing a plain re-cluster could not also satisfy.
        expectedFiles.values.max must beGreaterThan(1),
        filesPerEventName must beEqualTo(expectedFiles)
      ).reduce(_ and _)
    }
  }

  private def mkRow(schema: StructType, eventName: String): Row =
    Row.fromSeq(schema.fields.toSeq.map {
      case f if f.name == "event_name" => eventName
      case f if f.nullable             => null
      case f                           => placeholder(f.dataType)
    })

  private def placeholder(dataType: DataType): Any = dataType match {
    case StringType    => UUID.randomUUID.toString
    case TimestampType => new Timestamp(0L)
    case IntegerType   => 0
    case LongType      => 0L
    case DoubleType    => 0.0d
    case BooleanType   => false
    case other         => throw new IllegalStateException(s"No placeholder for a required field of type $other")
  }

  private def parquetFiles(root: File): List[File] =
    if (root.isDirectory) Option(root.listFiles()).toList.flatten.flatMap(parquetFiles)
    else List(root).filter(_.getName.endsWith(".parquet"))
}
