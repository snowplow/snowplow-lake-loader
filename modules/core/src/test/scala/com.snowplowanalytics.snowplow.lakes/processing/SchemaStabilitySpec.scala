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

import cats.Foldable
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.testing.specs2.CatsEffect
import cats.implicits._
import io.circe.Json
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.metrics.source.CodegenMetrics
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.{JavaNetRegistryLookup, RegistryLookup}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.loaders.transform.{NonAtomicFields, TabledEntity, Transform}

/**
 * Counts the Janino compiles behind each batch, which is what `EntitySuperset` exists to stop.
 *
 * Spark reuses generated code only when its source text is identical, so a batch costs a compile in
 * `encodeBatch` whenever its schema differs from every earlier one, and the union's per-batch
 * projections cost more at `prepareCommit`. Once the superset has settled, every batch has the same
 * schema whatever entities it carries, and neither should compile anything.
 *
 * The codegen cache is global to the JVM, so this starts from a cleared one, and the entities have
 * different shapes - an unstruct event, a context array and a nested struct - because generated
 * code depends on column types and positions rather than names.
 */
class SchemaStabilitySpec extends Specification with CatsEffect {
  import SchemaStabilitySpec._

  // It starts its own Spark session, which outlasts the default; a timeout reports as a skip.
  override val Timeout = 120.seconds

  def is = sequential ^ s2"""
  Batches resolved against the superset should:
    Stop compiling generated code in encodeBatch and prepareCommit once the superset has settled $e1
  """

  def e1 = withSpark.use { spark =>
    for {
      _ <- IO.blocking(clearCodegenCache())
      superset <- Ref[IO].of(Map.empty: EntitySuperset.Superset)
      // The first batch of the first window carries every entity, so the superset settles at once.
      window1 <- runWindow(spark, superset, 1, "test_schema_stability_w1", List(allEntities, onlyUe, onlyContext, ueAndNested, nothing))
      // Same window columns as window 1, but batches in a different mix and order.
      window2 <- runWindow(spark, superset, 2, "test_schema_stability_w2", List(ueAndNested, nothing, allEntities, onlyContext, onlyUe))
      // The same batches resolved alone, without the superset: each distinct entity mix is a
      // schema of its own. Shows the counts above can move at all.
      perBatch <- List(onlyUe, onlyContext, ueAndNested).traverse(encodePerBatch(spark, _))
    } yield {
      val (window1Encodes, window1Prepare) = window1
      val (window2Encodes, window2Prepare) = window2
      List[MatchResult[Any]](
        window1Encodes.head must beGreaterThan(0L),
        window1Encodes.tail must beEqualTo(List.fill(window1Encodes.size - 1)(0L)),
        window1Prepare must beGreaterThan(0L),
        window2Encodes must beEqualTo(List.fill(window2Encodes.size)(0L)),
        window2Prepare must beEqualTo(0L),
        perBatch must contain(beGreaterThan(0L)).forall
      ).reduce(_ and _)
    }
  }
}

object SchemaStabilitySpec {

  private implicit val lookup: RegistryLookup[IO] = JavaNetRegistryLookup.ioLookupInstance[IO]

  private val resolver     = Resolver[IO](Nil, None)
  private val badProcessor = BadRowProcessor("lake-loader-test", "0.0.0")

  private def compiles: Long = CodegenMetrics.METRIC_COMPILATION_TIME.getCount

  private def counting[A](io: IO[A]): IO[(A, Long)] =
    for {
      before <- IO(compiles)
      a <- io
      after <- IO(compiles)
    } yield (a, after - before)

  /**
   * Runs one window the way `Processing` and `LakeWriter` do, and returns the compiles of each
   * batch's `encodeBatch` and of the window's `prepareCommit`.
   */
  private def runWindow(
    spark: SparkSession,
    superset: Ref[IO, EntitySuperset.Superset],
    window: Int,
    viewName: String,
    batches: List[List[Event]]
  ): IO[(List[Long], Long)] =
    for {
      _ <- SparkUtils.initializeLocalDataFrame[IO](spark, viewName)
      perBatch <- batches.traverse { events =>
                    for {
                      entities <- IO(Foldable[List].foldMap(events)(TabledEntity.forEvent(_)))
                      resolved <-
                        EntitySuperset.resolve[IO](NonAtomicFields.resolveTypes[IO](resolver, _, Nil), Nil, superset, 500, window, entities)
                      r <- IO.fromEither(resolved)
                      schema = SparkSchema.forBatch(r.result.fields, respectIgluNullability = true)
                      rows <- IO(NonEmptyList.fromListUnsafe(transform(events, r.result)))
                      encoded <- counting(SparkUtils.encodeBatch[IO](spark, rows, schema))
                      (internalRows, encodeCompiles) = encoded
                      staged <- SparkUtils.stageBatch[IO](spark, internalRows, schema, stageOffHeap = false)
                      _ <- SparkUtils.appendStagedBatch[IO](spark, viewName, staged.df, schema, shouldRestoreNullability = true)
                    } yield (encodeCompiles, r.columnNames, staged.checkpointed)
                  }
      columns = perBatch.flatMap(_._2).toSet
      prepared <- counting(
                    for {
                      df <- SparkUtils.prepareFinalDataFrame[IO](
                              spark,
                              viewName,
                              columns,
                              writerParallelism = 2,
                              partitioning,
                              Some(Config.PartitionColumn.EventName),
                              eventCounts.values.sum,
                              eventCounts
                            )
                      shuffled <- SparkUtils.materializeShuffle[IO](spark, df)
                    } yield shuffled
                  )
      (shuffled, prepareCompiles) = prepared
      _ <- SparkUtils.dropView[IO](spark, viewName, perBatch.map(_._3): List[RDD[InternalRow]], shuffled.shuffle.map(_.id))
    } yield (perBatch.map(_._1), prepareCompiles)

  /**
   * Encodes one batch with only its own entities' schema, without the superset.
   */
  private def encodePerBatch(spark: SparkSession, events: List[Event]): IO[Long] =
    for {
      resolved <- NonAtomicFields.resolveTypes[IO](resolver, Foldable[List].foldMap(events)(TabledEntity.forEvent(_)), Nil)
      result <- IO.fromEither(resolved)
      schema: StructType = SparkSchema.forBatch(result.fields, respectIgluNullability = true)
      rows <- IO(NonEmptyList.fromListUnsafe(transform(events, result)))
      counted <- counting(SparkUtils.encodeBatch[IO](spark, rows, schema))
    } yield counted._2

  private def transform(events: List[Event], result: NonAtomicFields.Result): List[Row] =
    events.map { event =>
      Transform.transformEvent[Any](badProcessor, SparkCaster, event, result).map(SparkCaster.row(_)) match {
        case Right(row) => row
        case Left(bad)  => throw new IllegalStateException(s"Unexpected bad row: $bad")
      }
    }

  /**
   * Empties Spark's JVM-global cache of compiled classes, so the counts here do not depend on other
   * specs.
   */
  private def clearCodegenCache(): Unit = {
    val accessor: java.lang.reflect.Method = CodeGenerator.getClass.getDeclaredMethods
      .find(m => m.getParameterCount === 0 && m.getName.split('$').lastOption.contains("cache"))
      .getOrElse(throw new IllegalStateException("CodeGenerator has no cache accessor; has Spark changed?"))
    accessor.setAccessible(true)
    val cache: AnyRef = accessor.invoke(CodeGenerator)
    val _             = cache.getClass.getMethod("invalidateAll").invoke(cache)
  }

  private val partitioning = Config.WriterPartitioning(splitsPerFairShare = 4, minEventsPerSplit = 5000)
  private val eventCounts  = Map(Option.empty[String] -> 100)

  private def event(ue: Option[SelfDescribingData[Json]], contexts: List[SelfDescribingData[Json]]): Event =
    Event
      .minimal(UUID.randomUUID, Instant.now, "0.0.0", "0.0.0")
      .copy(unstruct_event = SnowplowEvent.UnstructEvent(ue), contexts = SnowplowEvent.Contexts(contexts))

  private val goodSchema = SelfDescribingData(
    SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
    Json.obj("col_a" -> Json.fromString("xyz"))
  )
  private val goodContext = SelfDescribingData(
    SchemaKey("myvendor", "goodcontext", "jsonschema", SchemaVer.Full(1, 0, 0)),
    Json.obj("col_a" -> Json.fromString("xyz"))
  )
  private val nested = SelfDescribingData(
    SchemaKey("myvendor", "nestedschema", "jsonschema", SchemaVer.Full(1, 0, 0)),
    Json.obj("inner" -> Json.obj("id" -> Json.fromString("id_val")))
  )

  private val allEntities = List(event(Some(goodSchema), List(goodContext)), event(Some(nested), Nil))
  private val onlyUe      = List(event(Some(goodSchema), Nil))
  private val onlyContext = List(event(None, List(goodContext)))
  private val ueAndNested = List(event(Some(nested), Nil), event(Some(goodSchema), Nil))
  private val nothing     = List(event(None, Nil))

  private def withSpark: Resource[IO, SparkSession] = {
    val build = IO.blocking(
      SparkSession
        .builder()
        .master("local[2]")
        .appName("SchemaStabilitySpec")
        .config("spark.memory.storageFraction", "0")
        // As in reference.conf, so prepareFinalDataFrame plans the exchange materializeShuffle runs.
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    Resource.make(build)(s => IO.blocking(s.close()))
  }
}
