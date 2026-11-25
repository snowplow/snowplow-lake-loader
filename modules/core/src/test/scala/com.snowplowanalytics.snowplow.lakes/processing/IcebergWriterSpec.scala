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
import org.apache.iceberg.Table
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.exceptions.{CommitFailedException, CommitStateUnknownException, RESTException}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.types.Types
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, spark_partition_id}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import fs2.io.file.Path

import com.snowplowanalytics.snowplow.runtime.Retrying
import com.snowplowanalytics.snowplow.lakes.{Config, TestConfig, TestSparkEnvironment}
import com.snowplowanalytics.snowplow.lakes.tables.{IcebergWriter, Writer}

// Uses SparkUtils (private[processing]), so this test must live in the same package.
class IcebergWriterSpec extends Specification with CatsEffect {

  override val Timeout = 120.seconds

  def is = sequential ^ s2"""
  IcebergWriter.write should:
    Write a partition whose rows are interleaved across event_name values $e1
    Write the DataFrame prepareCommit actually hands it, not just one built directly $e2
    Recover on the next attempt after a commit failed against a concurrently evolved schema $e3
    Write nothing new on a retry of a window whose tagged snapshot is already in the table $e7
    Write on a retry of a window that did not land $e8
    Stop looking for the window at this process's previous snapshot $e9
    Look past interleaved snapshots from other processes, and untagged ones $e10

  IcebergWriter.prepareTable should:
    Fail, and drop the cached table, when the catalog was asked to vend credentials and vended none $e6
    Tag its empty commit with this process's id $e11

  A retried commit through the LakeWriter should:
    Land the window exactly once when the catalog applies the commit then reports its state unknown $e12
    Land the window exactly once when the catalog applies the commit then fails the request $e13

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
        _ <- writer.prepareTable[IO](spark, processId)
        // One partition, so a single writer task meets both event_name values.
        df <- IO.blocking(spark.createDataFrame(rows.asJava, schema).coalesce(1))
        result <- writer.write[IO](df, windowId("v0000000001"), isRetry = false).attempt
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
        _ <- writer.prepareTable[IO](spark, processId)
        _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
        encoded <- SparkUtils.encodeBatch[IO](spark, rows, schema)
        staged <- SparkUtils.stageBatch[IO](spark, encoded, schema, stageOffHeap = false)
        _ <- SparkUtils.appendStagedBatch[IO](spark, viewName, staged.df, schema, shouldRestoreNullability = false)
        prepared <- SparkUtils.prepareFinalDataFrame[IO](
                      spark,
                      viewName,
                      nonAtomicColumnNames = Set.empty,
                      writerParallelism    = 2,
                      writerPartitioning   = config.spark.writerPartitioning,
                      eventNameCounts      = cycled.groupBy(name => Option(name)).view.mapValues(_.size).toMap
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
        result <- writer.write[IO](toWrite, windowId(viewName), isRetry = false).attempt
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
        _ <- asCreated.prepareTable[IO](spark, processId)
        _ <- asRestarted.prepareTable[IO](spark, processId)
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
        _ <- writer.prepareTable[IO](spark, processId)
        _ <- IO.blocking(spark.sql("ALTER TABLE iceberg_catalog.`test`.`events` WRITE ORDERED BY bucket(16, event_id) DESC"): Unit)
        described <- writer.describeTable[IO](spark)
      } yield described(0) must contain("sort order = [bucket[16](event_id) DESC NULLS LAST]")
    }
  }

  /**
   * Stands a `HadoopCatalog` in for the REST catalog: the REST catalog's options can override
   * `catalog-impl`, and `warehouse` comes from the catalog name. Its `ResolvingFileIO` holds no
   * credentials, which is what Unity Catalog's `loadTable` leaves when it vends none.
   *
   * The retry is the assertion that matters. The table this session holds before `prepareTable` is
   * the one `CachingCatalog` would hand a retry, so a fresh load afterwards must return a different
   * table. Asserts the cache holds it first, or a disabled cache would pass without testing
   * anything.
   */
  def e6 = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val config = TestConfig.defaults(TestConfig.Iceberg, tmpDir)
    val iceberg = config.output.good match {
      case i: Config.Iceberg =>
        i.copy(
          location = None,
          catalog = Config.IcebergCatalog.Rest(
            tmpDir.toNioPath.toUri,
            tmpDir.toNioPath.toString,
            Map(
              "catalog-impl" -> classOf[HadoopCatalog].getName,
              "header.X-Iceberg-Access-Delegation" -> "vended-credentials"
            )
          )
        )
      case other => throw new IllegalStateException(s"Expected an Iceberg target but got $other")
    }
    val writer  = new IcebergWriter(iceberg)
    val fqTable = "iceberg_catalog.`test`.`events`"

    SparkUtils.session[IO](config.spark, writer, iceberg, cores = 2).use { spark =>
      for {
        _ <- IO.blocking(spark.sql(s"CREATE TABLE $fqTable (${SparkSchema.ddlForCreate}) USING ICEBERG"): Unit)
        cached <- IO.blocking(Spark3Util.loadIcebergTable(spark, fqTable))
        cachedAgain <- IO.blocking(Spark3Util.loadIcebergTable(spark, fqTable))
        result <- writer.prepareTable[IO](spark, processId).attempt
        reloaded <- IO.blocking(Spark3Util.loadIcebergTable(spark, fqTable))
      } yield List(
        cachedAgain must beTheSameAs(cached),
        result must beLike { case Left(_: IcebergWriter.NoVendedCredentialsError) => ok },
        reloaded must not(beTheSameAs(cached))
      ).reduce(_ and _)
    }
  }

  /**
   * The case the lookup exists for: the attempt landed, but the loader saw it fail. Asserts the row
   * count as well as the snapshots, because a retry that wrote would add both.
   */
  def e7 = withIcebergWriter { (spark, writer) =>
    for {
      _ <- writer.prepareTable[IO](spark, processId)
      _ <- writer.write[IO](oneRow(spark), windowId("v0000000001"), isRetry = false)
      before <- tableState(spark)
      _ <- writer.write[IO](oneRow(spark), windowId("v0000000001"), isRetry = true)
      after <- tableState(spark)
    } yield (before, after) must beEqualTo(((1L, 2), (1L, 2)))
  }

  /** The startup commit is this process's own, so the walk stops there. */
  def e8 = withIcebergWriter { (spark, writer) =>
    for {
      _ <- writer.prepareTable[IO](spark, processId)
      _ <- writer.write[IO](oneRow(spark), windowId("v0000000001"), isRetry = true)
      after <- tableState(spark)
    } yield after must beEqualTo((1L, 2))
  }

  /**
   * The window is in the table, but behind another of this process's snapshots, so the walk stops
   * before reaching it and the window is written again. The loader never retries a window after
   * committing a later one, because `RemoteTable` holds its mutex across the retries; this pins
   * that the walk stops rather than searching the whole ancestry.
   */
  def e9 = withIcebergWriter { (spark, writer) =>
    for {
      _ <- writer.prepareTable[IO](spark, processId)
      _ <- writer.write[IO](oneRow(spark), windowId("v0000000001"), isRetry = false)
      _ <- writer.write[IO](oneRow(spark), windowId("v0000000002"), isRetry = false)
      _ <- writer.write[IO](oneRow(spark), windowId("v0000000001"), isRetry = true)
      after <- tableState(spark)
    } yield after must beEqualTo((3L, 4))
  }

  /**
   * Other pods commit to the same table between this window's attempts. Their snapshots carry
   * another process's id, or none if they come from something other than this loader.
   */
  def e10 = withIcebergWriter { (spark, writer) =>
    val otherProcess = Writer.WindowId(UUID.randomUUID, "v0000000001")
    for {
      _ <- writer.prepareTable[IO](spark, processId)
      _ <- writer.write[IO](oneRow(spark), windowId("v0000000001"), isRetry = false)
      _ <- writer.write[IO](oneRow(spark), otherProcess, isRetry = false)
      _ <- IO.blocking(oneRow(spark).write.format("iceberg").mode("append").saveAsTable(fqTable))
      before <- tableState(spark)
      _ <- writer.write[IO](oneRow(spark), windowId("v0000000001"), isRetry = true)
      after <- tableState(spark)
    } yield (before, after) must beEqualTo(((3L, 4), (3L, 4)))
  }

  def e11 = withIcebergWriter { (spark, writer) =>
    for {
      _ <- writer.prepareTable[IO](spark, processId)
      table <- loadRefreshed(spark)
      tag <- IO(table.currentSnapshot.summary.get(IcebergWriter.windowIdProperty))
    } yield tag must beEqualTo(Writer.WindowId.startup(processId).render)
  }

  def e12 = landsExactlyOnce(() => new CommitStateUnknownException(new RuntimeException("the catalog's response was lost")))

  def e13 = landsExactlyOnce(() => new RESTException("Error occurred while processing POST request"))

  /**
   * Through `LakeWriter`, so the retry comes from `RemoteTable` and the lookup from the attempt it
   * passes `isRetry` to. The catalog applies the window's commit and then throws, and the window
   * must then be in the table once: not missing, and not written a second time by the retry.
   *
   * Read back through a catalog of its own, because the loader's session has gone by then. Reads
   * the rows as well as the snapshots' tags and summaries, so that a commit whose data files were
   * cleaned up after it was applied cannot pass.
   */
  private def landsExactlyOnce(error: () => Throwable) = fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
    val viewName = "v0000000001"
    val config   = TestConfig.defaults(TestConfig.Iceberg, tmpDir)
    val iceberg = config.output.good match {
      case i: Config.Iceberg =>
        i.copy(
          location = None,
          catalog = Config.IcebergCatalog.Rest(
            tmpDir.toNioPath.toUri,
            tmpDir.toNioPath.toString,
            Map("catalog-impl" -> classOf[AmbiguousCommitCatalog].getName)
          )
        )
      case other => throw new IllegalStateException(s"Expected an Iceberg target but got $other")
    }
    val schema   = StructType(SparkSchema.atomic)
    val rows     = NonEmptyList.of(mkRow(schema, "a_event"), mkRow(schema, "b_event"))
    val counts   = Map[Option[String], Int](Some("a_event") -> 1, Some("b_event") -> 1)
    val eventIds = rows.toList.map(_.getString(schema.fieldIndex("event_id"))).sorted

    val lakeWriter = LakeWriter.build[IO](
      config.spark,
      iceberg,
      respectIgluNullability = true,
      cores                  = 2,
      TestSparkEnvironment.dummyAppHealth,
      Config.Retries(Retrying.Config.ForSetup(30.seconds), Retrying.Config.ForTransient(10.millis, 3)),
      PartialFunction.empty
    )

    val run = lakeWriter.use { w =>
      for {
        _ <- w.createTable
        _ <- IO(AmbiguousCommitCatalog.failNextWindowCommit(error))
        _ <- w.initializeLocalDataFrame(viewName)
        _ <- w.localAppendRows(viewName, rows, schema)
        _ <- w.prepareCommit(viewName, counts, Set.empty)
        _ <- w.commit(viewName)
        _ <- w.removeDataFrameFromDisk(viewName)
      } yield ()
    }

    for {
      result <- run.attempt.guarantee(IO(AmbiguousCommitCatalog.clear()))
      landed <- IO.blocking {
                  val catalog = new HadoopCatalog(new Configuration, tmpDir.toNioPath.toString)
                  try {
                    val table = catalog.loadTable(TableIdentifier.of(iceberg.database, iceberg.table))
                    val windows =
                      IcebergWriter.ancestry(table).map(s => Option(s.summary.get(IcebergWriter.windowIdProperty))).toList
                    val records = table.currentSnapshot.summary.get("total-records").toLong
                    val read    = IcebergGenerics.read(table).select("event_id").build()
                    val readIds =
                      try read.asScala.map(_.getField("event_id").toString).toList.sorted
                      finally read.close()
                    (windows.flatten.flatMap(Writer.WindowId.parse).map(_.window).count(_ == viewName), records, readIds)
                  } finally catalog.close()
                }
    } yield List(
      result must beRight,
      landed must beEqualTo((1, 2L, eventIds))
    ).reduce(_ and _)
  }

  private val fqTable = "iceberg_catalog.`test`.`events`"

  private def withIcebergWriter[A](f: (SparkSession, IcebergWriter) => IO[A]): IO[A] =
    fs2.io.file.Files[IO].tempDirectory.use { tmpDir =>
      val config = TestConfig.defaults(TestConfig.Iceberg, tmpDir)
      val iceberg = config.output.good match {
        case i: Config.Iceberg => i
        case other             => throw new IllegalStateException(s"Expected an Iceberg target but got $other")
      }
      val writer = new IcebergWriter(iceberg)
      SparkUtils.session[IO](config.spark, writer, iceberg, cores = 2).use(f(_, writer))
    }

  private def oneRow(spark: SparkSession): DataFrame = {
    val schema = SparkSchema.structForCreate
    spark.createDataFrame(List(mkRow(schema, "a_event")).asJava, schema)
  }

  private def loadRefreshed(spark: SparkSession): IO[Table] =
    IO.blocking(spark.sql(s"REFRESH TABLE $fqTable")) *> IO.blocking(Spark3Util.loadIcebergTable(spark, fqTable))

  /** The table's rows and its retained snapshots */
  private def tableState(spark: SparkSession): IO[(Long, Int)] =
    for {
      table <- loadRefreshed(spark)
      rows <- IO.blocking(spark.table(fqTable).count())
    } yield (rows, table.snapshots.asScala.size)

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
        _ <- writer.prepareTable[IO](spark, processId)
        _ <- addColumnOutOfBand(tmpDir, iceberg)
        first <- writer.write[IO](dfWithExtraColumn(spark), windowId("v0000000001"), isRetry = false).attempt
        // Not marked as a retry, although it stands for one: a retry's lookup refreshes the table
        // itself, which would pass this example whether or not the failed write dropped the cache.
        second <- writer.write[IO](dfWithExtraColumn(spark), windowId("v0000000001"), isRetry = false).attempt
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

  private val processId: UUID = UUID.randomUUID

  private def windowId(window: String): Writer.WindowId = Writer.WindowId(processId, window)

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
