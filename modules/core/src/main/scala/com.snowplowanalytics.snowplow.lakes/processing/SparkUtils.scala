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
import cats.effect.{Async, Sync}
import cats.effect.kernel.Resource
import cats.effect.implicits._
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SnowplowInternalSparkBridge, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.tables.Writer
import com.snowplowanalytics.snowplow.lakes.fs.LakeLoaderFileSystem

private[processing] object SparkUtils {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def session[F[_]: Async](
    config: Config.Spark,
    writer: Writer,
    target: Config.Target
  ): Resource[F, SparkSession] = {
    val builder =
      SparkSession
        .builder()
        .appName("snowplow-lake-loader")
        .master(s"local[*, ${config.taskRetries}]")
        .config(sparkConfigOptions(config, writer))

    val openLogF  = Logger[F].info("Creating the global spark session...")
    val closeLogF = Logger[F].info("Closing the global spark session...")
    val buildF    = Sync[F].delay(builder.getOrCreate())

    Resource
      .make(openLogF >> buildF)(s => closeLogF >> Sync[F].blocking(s.close()))
      .evalTap { session =>
        target match {
          case delta: Config.Delta =>
            Sync[F].delay {
              // Forces Spark to use `LakeLoaderFileSystem` when writing to the Lake via Hadoop
              // Delta tolerates async deletes; in other words when we delete a file, there is no strong
              // requirement that the file must be deleted immediately. Delta uses unique file names and never
              // re-writes a file that was previously deleted
              LakeLoaderFileSystem.overrideHadoopFileSystemConf(delta.location, session.sparkContext.hadoopConfiguration)
            }
          case _ => Sync[F].unit
        }
      }
  }

  private def sparkConfigOptions(config: Config.Spark, writer: Writer): Map[String, String] =
    writer.sparkConfig ++ config.conf

  /**
   * Whether to stage window batches in off-heap memory instead of on the JVM heap.
   *
   * Off-heap keeps a window's worth of staged events out of the garbage collector's way, but the
   * pool is sized separately from the heap and comes out of the same container memory limit, so it
   * is opt-in. Without a pool, `StorageLevel.OFF_HEAP` blocks would find no off-heap memory to
   * acquire and every staged block would go straight to disk, which on the deployments this targets
   * is already the bottleneck.
   *
   * Note that a pool is not a guarantee of residency: `spark.memory.storageFraction` is "0" in
   * reference.conf, so staged blocks own none of the pool. They borrow from the execution region
   * and are evicted back to disk whenever execution reclaims it. That is the trade we want -
   * starving execution fails a task, whereas an evicted block costs one write and one read and is
   * still off the heap, so it keeps the GC benefit that is the point of staging off-heap at all.
   * Some spilling under load is expected here, not a sign of misconfiguration.
   *
   * When sizing the pool, note that `spark.memory.offHeap.enabled` also puts Tungsten *execution*
   * memory off-heap, so the pool has to cover the commit job's shuffle as well as the staged
   * window. The pool is native memory allocated through `Unsafe`, outside every JVM budget - see
   * the `jdk.internal.ref` flag in `BuildSettings.javaModuleFlags`, without which it would count
   * against `-XX:MaxDirectMemorySize` instead.
   *
   * Decided once, because a `SparkConf` is fixed when the context is created. The choice is
   * otherwise invisible, hence the log line.
   */
  def stageBatchesOffHeap[F[_]: Sync](spark: SparkSession): F[Boolean] = {
    val enabled = spark.sparkContext.getConf.getBoolean("spark.memory.offHeap.enabled", false)
    val log =
      if (enabled)
        Logger[F].info("Staging window batches in off-heap memory")
      else
        Logger[F].info("Staging window batches on the JVM heap; set spark.memory.offHeap.enabled and .size to stage off-heap")
    log.as(enabled)
  }

  def initializeLocalDataFrame[F[_]: Sync](spark: SparkSession, viewName: String): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Initializing local DataFrame with name $viewName")
      _ <- Sync[F].blocking {
             try {
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
               spark.emptyDataFrame.createTempView(viewName)
             } finally
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
           }
    } yield ()

  /**
   * Converts a batch of rows into Spark's internal representation.
   *
   * Expensive, and deliberately separate from [[appendStagedBatch]] so that the caller can run it
   * outside the mutex that serializes appends. `LakeWriter` does exactly that, for this and for
   * [[stageBatch]]; only the view read-modify-write is left under the mutex.
   *
   * Encoding here rather than letting Spark do it inside the job matters for more than parallelism.
   * `createDataFrame(rdd: RDD[Row], schema)` leaves the batch as `Row` objects in the RDD, so the
   * task that Spark builds for the checkpoint job carries the whole batch as a graph of boxed
   * objects. That graph is Kryo-serialized on Spark's task-scheduler thread, inside
   * `TaskSchedulerImpl.resourceOffers`, which is synchronized and shared with every other job in
   * the session - so the cost lands on a single-threaded path that also gates the window commit.
   * `UnsafeRow` implements `KryoSerializable` and writes its backing buffer directly, so
   * pre-encoding turns that object walk into a byte copy.
   *
   * Note this does not reduce total CPU per event, and slightly increases it: `RDDScanExec` applies
   * an `UnsafeProjection` to whatever its RDD yields, so the pre-encoded rows are projected again
   * on the executor regardless. The win is entirely that the expensive half now runs in parallel
   * and off the single-threaded scheduler path, so do not "optimise" it away by encoding inside the
   * job again.
   */
  def encodeBatch[F[_]: Sync](
    spark: SparkSession,
    rows: NonEmptyList[Row],
    igluSchema: StructType
  ): F[NonEmptyList[InternalRow]] =
    Sync[F].delay(SnowplowInternalSparkBridge.rowEncoder(spark, igluSchema)).flatMap { toInternalRow =>
      // The encoder reuses one output row, so each result must be copied before the next call.
      // Wrapping each row in a delay lets the Cats Effect runtime cede between rows, as elsewhere
      // in the transform path.
      rows.traverse(row => Sync[F].delay(toInternalRow(row).copy()))
    }

  /**
   * One batch staged as a checkpointed DataFrame, together with the RDD holding its blocks.
   *
   * The two travel together because the caller needs both and they come from the same checkpoint:
   * [[appendStagedBatch]] unions `df` onto the accumulated view, while `checkpointed` is what
   * `LakeWriter` accumulates for the window so [[dropView]] can release the blocks when the window
   * is committed.
   */
  final case class StagedBatch(df: DataFrame, checkpointed: RDD[InternalRow])

  /**
   * Stages one batch as a checkpointed DataFrame, truncating its lineage.
   *
   * The checkpoint is not optional: it replaces the RDD's dependencies, making the
   * `ParallelCollectionRDD` that holds this batch's rows unreachable so it can be collected. A bare
   * `persist` would leave every batch of the window pinned for the whole window.
   *
   * This runs a Spark job, and like [[encodeBatch]] it deliberately sits outside the mutex that
   * serializes appends: the staged DataFrame is a pure function of the batch and never reads or
   * writes the accumulated view, so it cannot participate in the read-modify-write race the mutex
   * exists to prevent. Keeping it out matters because the block write - serializing and, for
   * off-heap, compressing every row - happens on an executor thread, so batches stage across cores
   * instead of one at a time. Do not fold this back into [[appendStagedBatch]].
   *
   * The storage level for each choice is picked inside `checkpointedDataFrame`, where the reason
   * both must keep a disk fallback is written down.
   */
  def stageBatch[F[_]: Sync](
    spark: SparkSession,
    rows: NonEmptyList[InternalRow],
    igluSchema: StructType,
    stageOffHeap: Boolean
  ): F[StagedBatch] =
    for {
      _ <- Logger[F].debug(s"Staging batch of ${rows.size} events")
      staged <- Sync[F].blocking {
                  // The pool is a thread-local read when the job is submitted, so it has to be set
                  // on this thread rather than inherited from the append that follows.
                  try {
                    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
                    // Stage each batch as one partition. Note this must not be `coalesce(1)`, which would
                    // label the plan `SinglePartition`; Spark 4.1's UnionExec then zips the accumulated
                    // batches into one partition instead of concatenating them. See spark.sql.unionOutputPartitioning.
                    val batchRdd = spark.sparkContext.parallelize(rows.toList, 1)
                    val (df, checkpointed) =
                      SnowplowInternalSparkBridge.checkpointedDataFrame(spark, batchRdd, igluSchema, offHeap = stageOffHeap)
                    StagedBatch(df, checkpointed)
                  } finally
                    spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
                }
    } yield staged

  /**
   * Appends an already-staged batch to the local DataFrame we are accumulating for this window.
   *
   * Callers must hold the append mutex: this reads the named view, unions onto it, and re-saves it
   * under the same name, so two concurrent calls would each union onto the same snapshot and one
   * batch of events would be lost. Everything expensive happened in [[encodeBatch]] and
   * [[stageBatch]] before the mutex was taken; what is left is metadata.
   */
  def appendStagedBatch[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    staged: DataFrame,
    igluSchema: StructType,
    shouldRestoreNullability: Boolean
  ): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Appending a staged batch to local DataFrame $viewName")
      _ <- Sync[F].blocking {
             try {
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
               val accumulatedSchema = spark.table(viewName).schema
               val united            = staged.unionByName(spark.table(viewName), allowMissingColumns = true)
               val result            = if (shouldRestoreNullability) restoreNullability(igluSchema, accumulatedSchema, united) else united
               result.createOrReplaceTempView(viewName)
             } finally
               spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
           }
    } yield ()

  def prepareFinalDataFrame[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    writerParallelism: Int
  ): F[DataFrame] =
    for {
      df <- Sync[F].pure(spark.table(viewName))
      df <- Sync[F].pure {
              // Create equally-balanced partitions, for which events with similar event_name are likely to be in the same partition.
              // This maximizes output file sizes, for a lake which is partitioned by event_name.
              if (writerParallelism > 1) df.repartitionByRange(writerParallelism, col("event_name"), col("event_id")) else df.coalesce(1)
            }
    } yield df.withColumn("load_tstamp", current_timestamp())

  // Spark's unionByName can incorrectly promote inner StructType fields to nullable when the two
  // DataFrames have different nested struct schemas (e.g. after an Iglu schema patch version adds a
  // new sub-field). Spark's Cast rejects nullable → non-null casts, so we cannot use withColumn+cast
  // to fix the metadata. Instead we reattach the corrected schema via internalCreateDataFrame,
  // bypassing both Spark's analysis checks and the InternalRow→Row→InternalRow roundtrip that the
  // public createDataFrame(rdd: RDD[Row], schema) API incurs (nullability is metadata-only: it is
  // never enforced at runtime).
  private def restoreNullability(
    igluSchema: StructType,
    accumulatedSchema: StructType,
    df: DataFrame
  ): DataFrame =
    if (igluSchema == df.schema) df // union did not widen any types; nothing to correct
    else {
      val corrected = restoreNullabilityInSchema(igluSchema, accumulatedSchema, df.schema)
      if (corrected == df.schema) df
      else SnowplowInternalSparkBridge.reattachSchema(df, corrected)
    }

  // Builds the corrected top-level schema for the union result.
  // For columns that appear in the Iglu schema (source of truth for the current batch),
  // nullability is corrected recursively via restoreNullabilityInField. Extra columns that only
  // appear in the union result are left untouched.
  private def restoreNullabilityInSchema(
    igluSchema: StructType,
    accumulatedSchema: StructType,
    unionSchema: StructType
  ): StructType = {
    val igluFieldsByName        = igluSchema.fields.map(f => f.name -> f).toMap
    val accumulatedFieldsByName = accumulatedSchema.fields.map(f => f.name -> f).toMap
    StructType(unionSchema.fields.map { unionField =>
      igluFieldsByName.get(unionField.name) match {
        case Some(igluField) =>
          unionField.copy(dataType =
            restoreNullabilityInField(igluField.dataType, accumulatedFieldsByName.get(unionField.name).map(_.dataType), unionField.dataType)
          )
        case None => unionField
      }
    })
  }

  // Returns a DataType that preserves the structure of `dfType` (which may contain extra
  // accumulated nested fields not present in `igluType`) but restores nullability from `igluType`
  // and `accumulatedType` for any fields present in all. StructType and ArrayType are handled
  // recursively; all other types are returned unchanged.
  private def restoreNullabilityInField(
    igluType: DataType,
    accumulatedType: Option[DataType],
    dfType: DataType
  ): DataType =
    (igluType, dfType) match {
      case (igluStruct: StructType, dfStruct: StructType) =>
        restoreNullabilityInStruct(igluStruct, accumulatedType.collect { case st: StructType => st }, dfStruct)
      case (ArrayType(igluArrayType, igluContainsNull), ArrayType(dfArrayType, _)) =>
        val accumulatedArray = accumulatedType.collect { case at: ArrayType => at }
        ArrayType(
          restoreNullabilityInField(igluArrayType, accumulatedArray.map(_.elementType), dfArrayType),
          igluContainsNull || accumulatedArray.exists(_.containsNull)
        )
      case _ => dfType
    }

  // Produces a StructType whose fields come from `dfStruct` (the union result, which may have extra
  // fields from the accumulated view), but where inner-field nullability is restored from `igluStruct`
  // and `accumulatedStruct` for fields that appear in all.
  // nullable = igluField.nullable || accumulatedField.nullable prevents incorrectly marking a field as
  // NOT NULL when the accumulated view already contains nulls for it (e.g. a prior batch processed a
  // more permissive schema version where the field was nullable).
  private def restoreNullabilityInStruct(
    igluStruct: StructType,
    accumulatedStruct: Option[StructType],
    dfStruct: StructType
  ): StructType = {
    val igluFieldsByName        = igluStruct.fields.map(f => f.name -> f).toMap
    val accumulatedFieldsByName = accumulatedStruct.map(_.fields.map(f => f.name -> f).toMap).getOrElse(Map.empty)
    StructType(dfStruct.fields.map { dfField =>
      igluFieldsByName.get(dfField.name) match {
        case Some(igluField) =>
          val accumulatedField = accumulatedFieldsByName.get(dfField.name)
          dfField.copy(
            dataType = restoreNullabilityInField(igluField.dataType, accumulatedField.map(_.dataType), dfField.dataType),
            nullable = igluField.nullable || accumulatedField.exists(_.nullable)
          )
        case None => dfField
      }
    })
  }

  /**
   * Removes the window's view and releases the checkpoint blocks it accumulated.
   *
   * Dropping the view only removes the catalog entry. The blocks staged by [[stageBatch]] are held
   * by the block manager until Spark's `ContextCleaner` unpersists them, and it only does that once
   * the RDD has been garbage collected - these RDDs survive a whole window, so they are promoted to
   * the old generation and wait for a full GC. Spark's own backstop for that is a scheduled
   * `System.gc()` every `spark.cleaner.periodicGC.interval`, which defaults to 30 minutes. In the
   * meantime the blocks sit in the storage pool, and once that is full they spill to
   * `spark.local.dir` - the same filesystem the window commit shuffles to.
   *
   * That backstop is weaker still on the off-heap path, which is the one this matters most for.
   * Off-heap blocks are not on the JVM heap, so filling the pool provokes no GC of its own; the
   * only thing that collects the RDDs holding them is a GC driven by heap pressure or by Spark's
   * periodic `System.gc()` - and staging off-heap exists precisely to take that pressure off the
   * heap.
   *
   * We know exactly when this data is dead, so release it here rather than waiting to be collected.
   */
  def dropView[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    checkpointed: List[RDD[InternalRow]]
  ): F[Unit] =
    Logger[F].info(s"Removing Spark data frame $viewName...") >>
      Sync[F]
        .blocking {
          try {
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
            val _ = spark.catalog.dropTempView(viewName)
          } finally
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
        }
        // Guaranteed, so that a failure to drop the view cannot skip the release: `LakeWriter` has
        // already dropped its own reference to these RDDs, so this is the only chance to release
        // them deterministically. Dropping first is still the right order - the catalog entry is
        // what holds the strong references, and the plan is unreadable once the blocks are gone.
        .guarantee(checkpointed.traverse_(releaseBlocks[F]))

  /**
   * Releases one batch's checkpoint blocks, logging rather than raising if it fails.
   *
   * Every RDD has to be attempted, so this must not propagate: nothing records these RDDs any more,
   * so a failure that escaped would skip the ones behind it and leak them. Nor is it worth failing
   * the window over, since Spark's `ContextCleaner` is still the backstop - failing to release
   * leaves us exactly where we were before this existed.
   *
   * No scheduler pool, unlike the rest of this file: `unpersistRDD` is a message to the block
   * manager and submits no job.
   */
  private def releaseBlocks[F[_]: Sync](rdd: RDD[InternalRow]): F[Unit] =
    Sync[F]
      .blocking(SnowplowInternalSparkBridge.releaseCheckpointBlocks(rdd))
      .handleErrorWith { e =>
        Logger[F].warn(e)(s"Could not release the cached blocks of RDD ${rdd.id}. Leaving them to Spark's ContextCleaner.")
      }
}
