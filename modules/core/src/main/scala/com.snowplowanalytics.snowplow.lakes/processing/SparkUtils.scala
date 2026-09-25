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
import org.apache.spark.sql.{Column, DataFrame, Row, SnowplowInternalSparkBridge, SparkSession}
import org.apache.spark.sql.SnowplowInternalSparkBridge.MaterializedShuffle
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{array, current_timestamp, get, hash, lit, pmod, when}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.tables.Writer
import com.snowplowanalytics.snowplow.lakes.fs.LakeLoaderFileSystem

private[processing] object SparkUtils {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  /**
   * @param cores
   *   How many task slots Spark gets. The loader's own `writerParallelism` is derived from the same
   *   number - see `LakeWriter.chooseWriterParallelism` - so that the reserved task slot
   *   CLAUDE.md's "The handover into Spark must never have to wait" section depends on is reserved
   *   by construction rather than by two independent reads of `availableProcessors` agreeing.
   *
   * `master` is applied after the config map, so a `spark.master` key in the user's `spark.conf`
   * cannot silently change the slot count out from under that derivation. Every other key in that
   * map still reaches Spark unvalidated.
   */
  def session[F[_]: Async](
    config: Config.Spark,
    writer: Writer,
    target: Config.Target,
    cores: Int
  ): Resource[F, SparkSession] = {
    val builder =
      SparkSession
        .builder()
        .appName("snowplow-lake-loader")
        .config(sparkConfigOptions(config, writer))
        .master(s"local[$cores, ${config.taskRetries}]")

    val warnMasterF = Sync[F].whenA(config.conf.contains("spark.master")) {
      Logger[F].warn(
        s"Ignoring spark.master=${config.conf("spark.master")} from configuration. The loader sets it from its own core count, " +
          "because writerParallelism is derived from the same number."
      )
    }
    val openLogF  = warnMasterF >> Logger[F].info("Creating the global spark session...")
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
   * Both levels store a batch serialized, so the choice is which budget the area comes out of and
   * whether those bytes face the collector - not how much of a window a byte of area holds.
   *
   * Note that a pool is not a guarantee of residency: `spark.memory.storageFraction` is "0" in
   * reference.conf, so staged blocks own none of the pool. They borrow from the execution region
   * and are evicted back to disk whenever execution reclaims it. That is the trade we want -
   * starving execution fails a task, whereas an evicted block costs one write and one read and is
   * still off the heap, so it keeps the GC benefit that is the point of staging off-heap at all.
   * Some spilling under load is expected here, not a sign of misconfiguration.
   *
   * When sizing the pool, note that `spark.memory.offHeap.enabled` also puts Tungsten *execution*
   * memory off-heap, so the pool has to cover the commit job's shuffle and the sort its write does
   * as well as the staged window. The sort is the larger of the two and the one that empties the
   * pool: every writer task sorts at once, so it asks for a window's rows rather than one
   * partition's, and in different units - blocks are serialized and compressed by
   * `spark.rdd.compress`, whereas the sort holds raw `UnsafeRow`s. `Writer.write` has the detail.
   * The pool is native memory allocated through `Unsafe`, outside every JVM budget - see the
   * `jdk.internal.ref` flag in `BuildSettings.javaModuleFlags`, without which it would count
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
   * exists to prevent. Keeping it out matters because the block write - serializing and compressing
   * every row - happens on an executor thread, so batches stage across cores instead of one at a
   * time. Do not fold this back into [[appendStagedBatch]].
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

  /**
   * A window's data after its shuffle has been run, with the id of the shuffle holding it.
   *
   * `shuffle` is `None` when the plan had no exchange to materialize - see
   * `SnowplowInternalSparkBridge.materializeShuffle` - in which case `df` is the DataFrame that was
   * passed in and nothing has been released.
   */
  final case class ShuffledWindow(df: DataFrame, shuffle: Option[MaterializedShuffle])

  /**
   * Runs the shuffle of the window's final DataFrame, so its staged batches can be released.
   *
   * Sets no scheduler pool: this is commit work, and the default pool is what leaves `pool1`'s
   * reserved task slot free for a batch handover. `pool1` is for the staging path, whose jobs are
   * each small enough not to occupy that slot for long. A commit is not.
   *
   * The two log lines bracket the phase, so the split between the shuffle and the write is visible
   * in driver logs alone - the same way the three staging lines bracket the per-batch path.
   */
  def materializeShuffle[F[_]: Sync](spark: SparkSession, df: DataFrame): F[ShuffledWindow] =
    for {
      _ <- Logger[F].debug("Materializing the shuffle of the window's final DataFrame")
      shuffled <- Sync[F].blocking {
                    val (shuffledDf, shuffle) = SnowplowInternalSparkBridge.materializeShuffle(spark, df)
                    ShuffledWindow(shuffledDf, shuffle)
                  }
      _ <- Logger[F].debug(s"Materialized the window's shuffle as id ${shuffled.shuffle.fold("<none>")(_.id.toString)}")
    } yield shuffled

  /** Re-saves the window's view to point at a different DataFrame. */
  def replaceView[F[_]: Sync](viewName: String, df: DataFrame): F[Unit] =
    Logger[F].debug(s"Replacing local DataFrame $viewName with the shuffled window") >>
      Sync[F].blocking(df.createOrReplaceTempView(viewName))

  /** Releases the checkpoint blocks of every staged batch in a window. */
  def releaseStagedBatches[F[_]: Sync](checkpointed: List[RDD[InternalRow]]): F[Unit] =
    checkpointed.traverse_(releaseBlocks[F])

  /**
   * What the block manager is holding, as (memory bytes, disk bytes), at the moment of the call.
   */
  def blockManagerUsage[F[_]: Sync](spark: SparkSession): F[(Long, Long)] =
    Sync[F].blocking(SnowplowInternalSparkBridge.blockManagerUsage(spark))

  /** The total size of everything Spark is holding under `spark.local.dir`. Walks directories. */
  def localDiskBytes[F[_]: Sync](spark: SparkSession): F[Long] =
    Sync[F].blocking(SnowplowInternalSparkBridge.localDiskBytes(spark))

  /**
   * The window's events, repartitioned for the writers.
   *
   * Deliberately does not assign `load_tstamp`; `readFinalDataFrame` does, and says why.
   */
  def prepareFinalDataFrame[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    writerParallelism: Int,
    writerPartitioning: Config.WriterPartitioning,
    eventNameCounts: Map[Option[String], Int]
  ): F[DataFrame] =
    Sync[F].delay(spark.table(viewName)).flatMap(repartitionForWriting(_, writerParallelism, writerPartitioning, eventNameCounts))

  /**
   * Redistributes the accumulated window into the partitions that will be written to the lake.
   *
   * The aim is partitions of similar size, so that no single task becomes the critical path of the
   * commit, without scattering an `event_name` so widely that the output files become fragmented.
   * `WriterPartitioner` decides where each key goes, from the histogram the window counted as its
   * events arrived, so the commit reads the window once: nothing here inspects the data to work out
   * where a row belongs. A partitioning that needs boundaries instead - `repartitionByRange` - has
   * Spark sample every input partition in a job of its own first, reading the window twice.
   *
   * `repartitionById` is the only repartition that takes the partition id outright; `repartition(n,
   * cols)` is hardcoded to `pmod(murmur3(cols), n)` and cannot express a chosen assignment. It
   * plans as an ordinary `ShuffleExchangeExec` over `ShufflePartitionIdPassThrough`, which is what
   * `materializeShuffle` has to find in order to release the window's staged batches.
   */
  private def repartitionForWriting[F[_]: Sync](
    df: DataFrame,
    writerParallelism: Int,
    writerPartitioning: Config.WriterPartitioning,
    eventNameCounts: Map[Option[String], Int]
  ): F[DataFrame] =
    if (writerParallelism <= 1)
      Sync[F].delay(df.coalesce(1))
    else
      Sync[F].delay(WriterPartitioner.plan(eventNameCounts, writerParallelism, writerPartitioning)).flatMap { plan =>
        Logger[F].debug(s"Writing window into ${plan.numPartitions} partitions. ${describePlan(plan)}") *>
          Sync[F].delay(df.repartitionById(plan.numPartitions, partitionIdColumn(df, plan)))
      }

  /**
   * A CASE over `event_name` giving each row the partition `WriterPartitioner` chose for its key.
   *
   * The `None` key is the rows where `event_name` is null, so it is matched with `isNull`: an
   * equality against null never holds.
   *
   * Every id this can produce is below `plan.numPartitions`, which the exchange relies on: Spark
   * wraps the id in a `pmod` of its own, so a breach would not throw - it would quietly fold ids
   * back into range and unbalance the commit.
   */
  private def partitionIdColumn(df: DataFrame, plan: WriterPartitioner.Plan): Column = {
    val eventName = df.col("event_name")
    val eventId   = df.col("event_id")

    // The array of partitions for a split key is foldable, so Catalyst's ConstantFolding replaces
    // it with a single literal array before the expression reaches codegen. Without that it would
    // allocate an ArrayData per row, on the largest keys in the window. `SparkUtilsSpec` pins it.
    def partitionFor(partitions: Vector[Int]): Column =
      if (partitions.length == 1) lit(partitions.head)
      else get(array(partitions.map(lit): _*), pmod(hash(eventId), lit(partitions.length)))

    // Branches are emitted in the order `plan.assignments` gives them, which is hottest first: a
    // CASE short-circuits, and each branch scanned re-reads event_name into a fresh UTF8String.
    // There is one branch per distinct event_name in the window, a count nothing here bounds, so a
    // row costs its own key's position in this order - which is what makes the ordering load
    // bearing rather than a tidiness. A window whose volume is spread evenly over many names pays
    // the most, because then no ordering makes the common case short.
    val branches = plan.assignments.foldLeft(Option.empty[Column]) { case (acc, (name, partitions)) =>
      val matches = name.fold(eventName.isNull)(eventName === lit(_))
      val target  = partitionFor(partitions)
      Some(acc.fold(when(matches, target))(_.when(matches, target)))
    }

    // The else branch is unreachable - the histogram was built from the same events as these rows,
    // and every key in it gets a branch - but a CASE needs one. Anything that stopped giving every
    // key a branch, such as capping the chain, would have to give this branch a real placement
    // rather than one partition. An empty assignment means a window that saw no events at all,
    // which `Processing.finalizeWindow` does not commit, and then this is all there is.
    branches.fold(lit(plan.fallbackPartition))(_.otherwise(lit(plan.fallbackPartition)))
  }

  /**
   * The whole plan, for debugging an unbalanced commit.
   *
   * Every `event_name` is listed with the number of pieces it was cut into, the ones left whole
   * included. An `x1` is not noise: `minEventsPerSplit` keeps a key whole when it holds fewer than
   * twice that many events, so an `x1` beside a lopsided `partitionLoads` is the explanation for
   * the imbalance rather than something to filter out. Pieces are not output files - two pieces of
   * a key share a partition whenever the packer finds that one lightest twice, which more pieces
   * than partitions forces and fewer still allows - so a key's files are at most the lesser of its
   * piece count and `numPartitions`, and routinely fewer.
   *
   * In the plan's own order, which is descending event count, so the keys that decide how long the
   * commit takes come first. Never empty: `Processing.finalizeWindow` only commits a window with
   * `numEvents > 0`, and every event contributes a key.
   */
  private def describePlan(plan: WriterPartitioner.Plan): String = {
    def render(name: Option[String]) = name.getOrElse("<null>")
    val pieces = plan.assignments.map { case (name, partitions) => s"${render(name)} x${partitions.length}" }
    s"Partitions per event_name: ${pieces.mkString(", ")}. Expected events per partition: ${plan.partitionLoads.mkString(",")}"
  }

  /**
   * Reads back the window's view and stamps it with `load_tstamp`, ready to write.
   *
   * The stamp is assigned here, on the commit side of the handover, rather than in
   * `prepareFinalDataFrame` before the shuffle. `ComputeCurrentTime` folds `current_timestamp()` to
   * a literal when a plan is optimized, so assigning it earlier would fix the value in the
   * DataFrame that `prepareCommit` registers under the view name, and every retry of the write
   * would reuse it. A window whose commit is retried on the setup path waits for the customer to
   * fix their destination, with no attempt cap, so that value could be arbitrarily old by the time
   * the rows land. Reading it here means each attempt builds its own plan and stamps the rows with
   * the time that attempt started.
   *
   * Assigning it after the repartition, either way, is what keeps the sort cheap and the writers'
   * open-file count down - `Writer.write` has that argument, and it depends only on the value being
   * one literal per write, which it still is.
   */
  def readFinalDataFrame[F[_]: Sync](spark: SparkSession, viewName: String): F[DataFrame] =
    Sync[F].delay(spark.table(viewName).withColumn("load_tstamp", current_timestamp()))

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
   * Removes the window's view and releases what the window still holds inside Spark: the checkpoint
   * blocks it accumulated, or the shuffle that replaced them once `prepareCommit` has run.
   *
   * Dropping the view only removes the catalog entry. The blocks staged by [[stageBatch]] are held
   * by the block manager until Spark's `ContextCleaner` unpersists them, and it only does that once
   * the RDD has been garbage collected - these RDDs survive a whole window, so they are promoted to
   * the old generation and wait for a full GC. Spark's own backstop for that is a scheduled
   * `System.gc()` every `spark.cleaner.periodicGC.interval`, which defaults to 30 minutes. In the
   * meantime the blocks sit in the storage pool, and once that is full they spill to
   * `spark.local.dir` - the same filesystem the window commit shuffles and sorts to.
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
    checkpointed: List[RDD[InternalRow]],
    shuffleId: Option[Int]
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
        //
        // Which of the two holds the window's data depends on how far it got. Where `prepareCommit`
        // found a shuffle, `checkpointed` is empty and `shuffleId` is set; where the plan had no
        // exchange, or the window failed before that point, it is the other way round. Both are
        // attempted because this is the one place that has to cover every case.
        .guarantee(releaseStagedBatches[F](checkpointed) >> shuffleId.traverse_(releaseShuffle[F](spark, _)))

  /**
   * Releases one batch's checkpoint blocks, logging rather than raising if it fails.
   *
   * Every RDD has to be attempted, so this must not propagate: nothing records these RDDs any more,
   * so a failure that escaped would skip the ones behind it and leak them. Nor is it worth failing
   * the window over, since Spark's `ContextCleaner` is still the backstop - failing to release
   * leaves us exactly where we were before this existed.
   *
   * No scheduler pool: `unpersistRDD` is a message to the block manager and submits no job.
   */
  private def releaseBlocks[F[_]: Sync](rdd: RDD[InternalRow]): F[Unit] =
    Sync[F]
      .blocking(SnowplowInternalSparkBridge.releaseCheckpointBlocks(rdd))
      .handleErrorWith { e =>
        Logger[F].warn(e)(s"Could not release the cached blocks of RDD ${rdd.id}. Leaving them to Spark's ContextCleaner.")
      }

  /**
   * Releases a window's shuffle, logging rather than raising if it fails.
   *
   * Must not propagate, for the same reason as `releaseBlocks`: it runs inside `dropView`'s
   * `guarantee` alongside the block release, and an error escaping either would skip the other.
   * Spark's `ContextCleaner` is still the backstop, so a failure here costs a delayed cleanup
   * rather than a leak.
   */
  private def releaseShuffle[F[_]: Sync](spark: SparkSession, shuffleId: Int): F[Unit] =
    Sync[F]
      .blocking(SnowplowInternalSparkBridge.releaseShuffle(spark, shuffleId))
      .handleErrorWith { e =>
        Logger[F].warn(e)(s"Could not release the files of shuffle $shuffleId. Leaving them to Spark's ContextCleaner.")
      }
}
