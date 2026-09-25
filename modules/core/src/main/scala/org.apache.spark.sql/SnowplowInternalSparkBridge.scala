/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package org.apache.spark.sql

import org.apache.spark.rdd.{LocalRDDCheckpointData, RDD, RDDCheckpointData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.util.control.NonFatal

// Intentionally placed in org.apache.spark.sql. The methods below reach into Spark internals that
// scalac restricts with a private[sql] or private[spark] qualifier - all of them are public JVM
// members, the qualifier being enforced from the pickled Scala signature - and this package is
// inside both scopes. Each method names the internals it uses in its own scaladoc, rather than
// this comment carrying a list of them that would fall out of date as methods come and go.
//
// The casts to `classic` are safe because those internals exist only on the classic (non-Connect)
// implementation of the Spark SQL API, and the loader always runs a local classic session.
//
// Not part of the public API of this project.
object SnowplowInternalSparkBridge {

  /**
   * Re-labels a DataFrame with a different schema, leaving its rows untouched.
   *
   * Uses `internalCreateDataFrame` (`private[sql]`), which takes the existing `RDD[InternalRow]` as
   * it is. `SparkUtils.restoreNullability`, its only caller, explains why that is what we want.
   */
  def reattachSchema(df: DataFrame, schema: StructType): DataFrame = {
    val classicDf = df.asInstanceOf[classic.Dataset[Row]]
    classicDf.sparkSession.internalCreateDataFrame(classicDf.queryExecution.toRdd, schema)
  }

  /**
   * Builds the function that converts a `Row` into Spark's internal representation.
   *
   * `lenient = true` matches what `createDataFrame(rdd: RDD[Row], schema)` does internally, and is
   * required here: `SparkCaster` emits `java.time.Instant` and `java.time.LocalDate`, which the
   * strict encoders reject unless `spark.sql.datetime.java8API.enabled` is set.
   *
   * The returned function is NOT thread-safe and reuses a single output row, so callers must use
   * one instance per batch and copy each result before requesting the next.
   */
  def rowEncoder(spark: SparkSession, schema: StructType): Row => InternalRow = {
    // Built under withActive so the encoder resolves against this session's SQLConf, not the
    // fallback conf. `datetimeJava8ApiEnabled` and `ansiEnabled` are read here, at construction time.
    val serializer = spark.withActive(ExpressionEncoder(schema, lenient = true).createSerializer())

    // The serializer builds its UnsafeProjection lazily, on the first call rather than at
    // construction, and that build reads `spark.sql.codegen.factoryMode`. Callers invoke this
    // function on a Cats Effect compute thread, where SQLConf.get resolves via the active session
    // and so falls back to a fresh SQLConf when there is none. Keep every call under withActive
    // too; it costs two thread-local writes per row. It cannot be hoisted around the whole batch:
    // encodeBatch wraps each row in its own delay, so the fiber may change carrier thread between
    // rows, and withActive is a thread-local.
    row => spark.withActive(serializer(row))
  }

  /**
   * Wraps an RDD of already-encoded rows as a DataFrame, with no further conversion.
   *
   * Uses `internalCreateDataFrame` (`private[sql]`), which takes the `RDD[InternalRow]` as it is,
   * rather than the public `createDataFrame(rdd: RDD[Row], schema)`, which would roundtrip every
   * row out to `Row` and back.
   */
  def fromInternalRows(
    spark: SparkSession,
    rows: RDD[InternalRow],
    schema: StructType
  ): DataFrame =
    spark.asInstanceOf[classic.SparkSession].internalCreateDataFrame(rows, schema)

  /**
   * [[fromInternalRows]] over an eagerly local-checkpointed copy of `rows`, together with the RDD
   * holding the checkpoint's blocks.
   *
   * Handing that RDD back is half the reason to checkpoint here rather than through
   * `Dataset.localCheckpoint()`. The caller needs it to release the blocks when the window is
   * dropped, and this is the one place it exists as a value; recovering it from the returned
   * DataFrame instead would mean asserting on the shape of a plan Spark builds and we do not
   * control.
   *
   * The other half is that `Dataset.localCheckpoint(eager, StorageLevel.OFF_HEAP)` cannot express
   * an off-heap level at all. It reaches `RDD.localCheckpoint()`, which normalises the level
   * through `LocalRDDCheckpointData.transformStorageLevel`. That rebuilds the level with
   * `StorageLevel(useDisk, useMemory, deserialized, replication)` - an overload with no
   * `useOffHeap` parameter - so `OFF_HEAP` reaches the block manager as `MEMORY_AND_DISK_SER`, with
   * no warning and correct results. `SnowplowInternalSparkBridgeSpec` pins that behaviour, so it
   * fails if a Spark upgrade makes this method unnecessary.
   *
   * So we set the level and install the checkpoint data ourselves, which is all
   * `RDD.localCheckpoint()` does apart from the normalisation. That means reaching
   * `RDD.checkpointData`, `RDD.doCheckpoint` and `LocalRDDCheckpointData`, all `private[spark]`.
   *
   * Note that `transformStorageLevel` does two things, and only one of them is damage. Dropping
   * `useOffHeap` is the bug we route around; hardcoding `useDisk = true` is a safety property we
   * have to reproduce. A local checkpoint truncates the RDD's lineage, so a memory block evicted
   * with no disk fallback cannot be recomputed - the events would simply be gone.
   *
   * Spark enforces that property in two places, and we bypass only one of them. The other is an
   * `assume(level.useDisk)` at the top of `LocalRDDCheckpointData.doCheckpoint`, which this method
   * calls, so a level without a disk fallback throws before any block reaches the block manager
   * rather than losing events quietly. The level is chosen here from the two that satisfy the
   * property, rather than accepted as a parameter, so that neither check can be reached in anger.
   */
  def checkpointedDataFrame(
    spark: SparkSession,
    rows: RDD[InternalRow],
    schema: StructType,
    offHeap: Boolean
  ): (DataFrame, RDD[InternalRow]) = {
    // Both have useDisk, per the note above. MEMORY_AND_DISK is also what `Dataset.localCheckpoint`
    // would have used, so the heap path stages where it always did.
    val level = if (offHeap) StorageLevel.OFF_HEAP else StorageLevel.MEMORY_AND_DISK

    // Checkpoint a new RDD rather than `rows` itself. Completing the checkpoint calls
    // `markCheckpointed`, which clears this RDD's dependencies and so drops the reference to
    // `rows`, a ParallelCollectionRDD holding the whole batch of InternalRows in a field.
    // Persisting `rows` directly would leave every batch of the window reachable from the
    // accumulated view's plan.
    //
    // No `map(_.copy())`, unlike Dataset.checkpoint: that copies because it checkpoints
    // `physicalPlan.execute()`, whose RDDScanExec reuses one UnsafeProjection output row. These
    // rows come from SparkUtils.encodeBatch, already copied, and nothing reuses them.
    val checkpointable = rows.mapPartitions(identity)

    RDDCheckpointData.synchronized {
      // Persist first. `RDD.persist` checks `isLocallyCheckpointed`, so once `checkpointData` is
      // set it routes the level through `transformStorageLevel` and drops useOffHeap - the very
      // bug this method exists to avoid. Swapping these two lines reintroduces it.
      checkpointable.persist(level)
      checkpointable.checkpointData = Some(new LocalRDDCheckpointData(checkpointable))
    }

    // Materialise the blocks now, at whichever level. This is what Spark itself calls for
    // `eager = true`. On failure the RDD is unreachable but still holds its blocks, and nothing
    // will reclaim them until a GC runs the ContextCleaner - which off-heap blocks, allocated
    // outside the heap, do not provoke at all. Release them here, on either path.
    //
    // Released through `releaseCheckpointBlocks` rather than `RDD.unpersist`, for the reason given
    // there: `checkpointData` is already installed by this point, so `isLocallyCheckpointed` is
    // true even though the checkpoint never completed, and `unpersist` would log the
    // cannot-be-recomputed warning next to the real failure.
    try checkpointable.doCheckpoint()
    catch {
      case NonFatal(t) =>
        releaseCheckpointBlocks(checkpointable)
        throw t
    }

    (fromInternalRows(spark, checkpointable, schema), checkpointable)
  }

  /**
   * Releases the cached blocks of an RDD returned by [[checkpointedDataFrame]].
   *
   * Not `RDD.unpersist`, which is otherwise identical: it warns that a locally checkpointed RDD
   * cannot be recomputed once unpersisted, and Spark logs that against the concrete RDD class -
   * `MapPartitionsRDD`, which is the commonest RDD type in Spark SQL. Silencing that logger would
   * also silence the warnings that tell us `localCheckpoint` had stopped doing its job, which is
   * the one failure this whole mechanism depends on hearing about. `unpersist` calls exactly this
   * after warning, so going straight to `SparkContext.unpersistRDD` (`private[spark]`) keeps the
   * release and drops only the message.
   *
   * The `storageLevel = NONE` that `unpersist` also does is skipped, which is moot: these RDDs are
   * never persisted again, they are released once and discarded. `unpersistRDD` still removes the
   * RDD from `SparkContext.getPersistentRDDs`, which is what `SparkUtilsSpec` asserts on.
   *
   * Non-blocking: the block manager removes the blocks asynchronously, and the next window is
   * already accumulating by the time this runs.
   */
  def releaseCheckpointBlocks(rdd: RDD[InternalRow]): Unit =
    rdd.sparkContext.unpersistRDD(rdd.id, blocking = false)
}
