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
import org.specs2.Specification

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.exceptions.CommitFailedException
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.types.Types
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, spark_partition_id}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import fs2.io.file.Path

import com.snowplowanalytics.snowplow.lakes.{Config, TestConfig}
import com.snowplowanalytics.snowplow.lakes.tables.IcebergWriter

// Uses SparkUtils (private[processing]), so this test must live in the same package.
class IcebergWriterSpec extends Specification with CatsEffect {

  override val Timeout = 120.seconds

  def is = sequential ^ s2"""
  IcebergWriter.write should:
    Write a partition whose rows are interleaved across event_name values $e1
    Write the DataFrame prepareCommit actually hands it, not just one built directly $e2
    Recover on the next attempt after a commit failed against a concurrently evolved schema $e3

  IcebergWriter.describeTable should:
    Report the properties of the existing table, not the ones this loader is configured with $e4
    Report a sort order set on the table $e5
  """

  /**
   * Pins that Spark still inserts the sort Iceberg's clustered writer requires - see `Writer.write`
   * for why the write depends on it. Nothing in the loader sorts, so the ordering comes entirely
   * from Spark's `V2Writes` rule acting on the requirement Iceberg declares.
   *
   * The other examples cannot catch this: every event they write leaves `event_name` unset, so the
   * table has a single partition value. The rows here are interleaved rather than merely distinct,
   * because a writer that meets each partition value in one contiguous run never reopens a file and
   * would pass unsorted.
   */
  def e1 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = TestConfig.defaults(TestConfig.Iceberg, tmpDir)
    val iceberg = config.output.good match {
      case i: Config.Iceberg => i
      case other             => throw new IllegalStateException(s"Expected an Iceberg target but got $other")
    }
    val writer = new IcebergWriter(iceberg)

    SparkUtils.session[IO](config.spark, writer, iceberg, cores = 2).use { spark =>
      val schema = SparkSchema.structForCreate
      val rows   = List("a_event", "b_event", "a_event").map(mkRow(schema, _))

      for {
        _ <- writer.prepareTable[IO](spark)
        // One partition, so a single writer task meets both event_name values.
        df <- IO.blocking(spark.createDataFrame(rows.asJava, schema).coalesce(1))
        result <- writer.write[IO](df).attempt
        written <- IO.blocking {
                     spark
                       .table("iceberg_catalog.`test`.`events`")
                       .select("event_name")
                       .collect()
                       .toList
                       .map(_.getString(0))
                   }
      } yield List(
        result must beRight,
        written must containTheSameElementsAs(List("a_event", "b_event", "a_event"))
      ).reduce(_ and _)
    }
  }

  /**
   * The same ordering property as `e1`, but over the DataFrame the loader actually produces: staged
   * batches, repartitioned, handed through `materializeShuffle` and the window's view, and only
   * then written. That composition is what production runs and `e1` does not reach - it builds its
   * input with `createDataFrame`, which advertises no partitioning and carries no relabelled
   * schema.
   *
   * `writerParallelism` is passed rather than derived from a core count, so that the branch this
   * example is about does not depend on what the runner has. `TestSparkEnvironment` pins four cores
   * for the same reason, so the whole-loader specs reach the exchange too; this one states the
   * value it needs rather than inheriting that choice.
   *
   * Asserts the interleaving it relies on rather than assuming it: the repartition clusters by
   * `event_name`, so with too few distinct values each partition would hold a single one and the
   * clustered writer would never reopen a file, leaving the example unable to fail.
   */
  def e2 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val viewName = "test_prepare_commit_composition_e2"
    val config   = TestConfig.defaults(TestConfig.Iceberg, tmpDir)
    val iceberg = config.output.good match {
      case i: Config.Iceberg => i
      case other             => throw new IllegalStateException(s"Expected an Iceberg target but got $other")
    }
    val writer = new IcebergWriter(iceberg)

    // Cycled, so that whichever way the partitioner splits these, a partition holding two of them
    // holds them interleaved rather than in contiguous runs.
    val eventNames = List("a_event", "b_event", "c_event", "d_event").flatMap(List.fill(3)(_)).sorted
    val cycled     = List.tabulate(eventNames.size)(i => eventNames(i % 4 * 3 + i / 4))

    SparkUtils.session[IO](config.spark, writer, iceberg, cores = 2).use { spark =>
      val schema = StructType(SparkSchema.atomic)
      val rows   = NonEmptyList.fromListUnsafe(cycled.map(mkRow(schema, _)))

      for {
        _ <- writer.prepareTable[IO](spark)
        _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
        encoded <- SparkUtils.encodeBatch[IO](spark, rows, schema)
        staged <- SparkUtils.stageBatch[IO](spark, encoded, schema, stageOffHeap = false)
        _ <- SparkUtils.appendStagedBatch[IO](spark, viewName, staged.df, schema, shouldRestoreNullability = false)
        prepared <- SparkUtils.prepareFinalDataFrame[IO](
                      spark,
                      viewName,
                      writerParallelism  = 2,
                      writerPartitioning = config.spark.writerPartitioning,
                      eventNameCounts    = cycled.groupBy(name => Option(name)).view.mapValues(_.size).toMap
                    )
        shuffled <- SparkUtils.materializeShuffle[IO](spark, prepared)
        _ <- SparkUtils.replaceView[IO](viewName, shuffled.df)
        toWrite <- SparkUtils.readFinalDataFrame[IO](spark, viewName)
        interleaved <- IO.blocking {
                         toWrite
                           .select(spark_partition_id().as("pid"), col("event_name"))
                           .collect()
                           .toList
                           .groupBy(_.getInt(0))
                           .values
                           .exists(rows => hasNonContiguousRun(rows.map(_.getString(1)).toList))
                       }
        result <- writer.write[IO](toWrite).attempt
        written <- IO.blocking {
                     spark
                       .table("iceberg_catalog.`test`.`events`")
                       .select("event_name")
                       .collect()
                       .toList
                       .map(_.getString(0))
                   }
      } yield List(
        shuffled.shuffle must beSome,
        interleaved must beTrue,
        result must beRight,
        written must containTheSameElementsAs(cycled)
      ).reduce(_ and _)
    }
  }

  /**
   * A restarted loader's configured properties are not the table's - see `Writer.describeTable`.
   *
   * The second writer runs `prepareTable` first, so what is described is the state a restart leaves
   * behind rather than a table the writer never touched.
   */
  def e4 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = TestConfig.defaults(TestConfig.Iceberg, tmpDir)
    val iceberg = config.output.good match {
      case i: Config.Iceberg => i
      case other             => throw new IllegalStateException(s"Expected an Iceberg target but got $other")
    }
    def withRetries(n: String): Config.Iceberg =
      iceberg.copy(icebergTableProperties = iceberg.icebergTableProperties + ("commit.retry.num-retries" -> n))

    val asCreated   = new IcebergWriter(withRetries("42"))
    val asRestarted = new IcebergWriter(withRetries("7"))

    SparkUtils.session[IO](config.spark, asCreated, iceberg, cores = 2).use { spark =>
      for {
        _ <- asCreated.prepareTable[IO](spark)
        _ <- asRestarted.prepareTable[IO](spark)
        described <- asRestarted.describeTable[IO](spark)
        structure  = described(0)
        properties = described(1)
      } yield List(
        properties must contain("commit.retry.num-retries=\"42\""),
        properties must not contain "commit.retry.num-retries=\"7\"",
        structure must contain("format version = 2"),
        structure must contain("partitioned by [day(load_tstamp) AS load_tstamp_day, identity(event_name) AS event_name]"),
        structure must contain("sort order = unsorted")
      ).reduce(_ and _)
    }
  }

  /** The loader sets no sort order, so `e4` pins only `unsorted`, and no transform at all. */
  def e5 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = TestConfig.defaults(TestConfig.Iceberg, tmpDir)
    val iceberg = config.output.good match {
      case i: Config.Iceberg => i
      case other             => throw new IllegalStateException(s"Expected an Iceberg target but got $other")
    }
    val writer = new IcebergWriter(iceberg)

    SparkUtils.session[IO](config.spark, writer, iceberg, cores = 2).use { spark =>
      for {
        _ <- writer.prepareTable[IO](spark)
        _ <- IO.blocking(spark.sql("ALTER TABLE iceberg_catalog.`test`.`events` WRITE ORDERED BY bucket(16, event_id) DESC"): Unit)
        described <- writer.describeTable[IO](spark)
      } yield described(0) must contain("sort order = [bucket[16](event_id) DESC NULLS LAST]")
    }
  }

  /** Whether any value appears again after a different one has intervened. */
  private def hasNonContiguousRun(values: List[String]): Boolean = {
    val compressed = values.foldRight(List.empty[String]) { (v, acc) =>
      if (acc.headOption.contains(v)) acc else v :: acc
    }
    compressed.size > compressed.distinct.size
  }

  /**
   * Reproduces concurrent schema evolution: another writer adds a column between this loader
   * resolving the table and committing a schema change of its own.
   *
   * The second write is the assertion, and stands for a retry of the first. Without an invalidation
   * it is planned against the same cached metadata and fails identically.
   *
   * Relies on the catalog cache, which `catalog.options` can disable. With it off both writes would
   * succeed and this would pass without testing anything.
   */
  def e3 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = TestConfig.defaults(TestConfig.Iceberg, tmpDir)
    val iceberg = config.output.good match {
      case i: Config.Iceberg => i
      case other             => throw new IllegalStateException(s"Expected an Iceberg target but got $other")
    }
    val writer = new IcebergWriter(iceberg)
    SparkUtils.session[IO](config.spark, writer, iceberg, cores = 2).use { spark =>
      for {
        _ <- writer.prepareTable[IO](spark)
        _ <- addColumnOutOfBand(tmpDir, iceberg)
        first <- writer.write[IO](dfWithExtraColumn(spark)).attempt
        second <- writer.write[IO](dfWithExtraColumn(spark)).attempt
        columns <- IO.blocking(spark.table("iceberg_catalog.`test`.`events`").schema.fieldNames.toList)
      } yield List(
        // Asserts the type, so that a disabled cache fails here instead of turning both writes green
        first must beLike { case Left(_: CommitFailedException) => ok },
        second must beRight,
        columns must contain("out_of_band_column"),
        columns must contain("loader_added_column")
      ).reduce(_ and _)
    }
  }

  /**
   * Filled by field name rather than positionally, so a change to the atomic field list cannot
   * silently shift values into the wrong columns.
   */
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

  /** An empty batch still merges its schema into the table, which is the commit under test */
  private def dfWithExtraColumn(spark: SparkSession): DataFrame = {
    val schema = SparkSchema.structForCreate.add("loader_added_column", StringType, nullable = true)
    spark.createDataFrame(List.empty[Row].asJava, schema)
  }

  /** Evolves the table through a catalog of its own, so the loader's session cannot see it */
  private def addColumnOutOfBand(tmpDir: Path, iceberg: Config.Iceberg): IO[Unit] =
    IO.blocking {
      val catalog = new HadoopCatalog(new Configuration, tmpDir.toNioPath.toString)
      try
        catalog
          .loadTable(TableIdentifier.of(iceberg.database, iceberg.table))
          .updateSchema()
          .addColumn("out_of_band_column", Types.StringType.get())
          .commit()
      finally catalog.close()
    }
}
