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
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ThreadUtils

import scala.concurrent.duration.Duration
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
    // Both have useDisk, per the note above, and both store a batch serialized, so the two paths
    // differ in which budget the area comes out of and nothing else. Deliberately not
    // `MEMORY_AND_DISK`, which is what a plain `Dataset.localCheckpoint()` picks: holding the batch
    // as live rows would make the old generation trace an object per row rather than a handful of
    // buffers per block, make evicting a block a re-encode on whichever commit thread reclaimed the
    // memory rather than a write of bytes that already exist, and make a block read back from disk
    // unroll into objects before it could be re-cached.
    val level = if (offHeap) StorageLevel.OFF_HEAP else StorageLevel.MEMORY_AND_DISK_SER

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
   * `diskBytes` is the sum of `MapOutputStatistics.bytesByPartitionId`, which is an upper bound on
   * the data files rather than their size: `MapStatus.compressSize` rounds each block up through
   * `ceil(log(size) / log(1.1))` into a byte, so a decompressed size overstates by up to 10%. That
   * is a property of `CompressedMapStatus` rather than of Spark - above
   * `spark.shuffle.minNumPartitionsToHighlyCompress` reduce partitions Spark switches to
   * `HighlyCompressedMapStatus`, which averages small blocks and can understate.
   * `writerParallelism` is `cores - 1`, so that threshold is out of reach here. It also excludes
   * the index files, 8 * (partitions + 1) bytes per map task. Both errors are small next to a
   * window's data, but the figure is an estimate and should not be differenced against a measured
   * one.
   *
   * Still the only measure of shuffle occupancy the loader has: shuffle files are never registered
   * as blocks with the `BlockManagerMaster`, so `blockManagerUsage` cannot see them at all.
   */
  final case class MaterializedShuffle(id: Int, diskBytes: Long)

  /**
   * Runs the plan's shuffle, and returns a DataFrame that reads its output plus the shuffle's id.
   *
   * The point is the boundary this creates. Once the map stage has completed, the shuffle files
   * hold everything the rest of the query needs, so the caller can release the staged batches the
   * plan was reading from without waiting for the write. `LakeWriter.prepareCommit` does exactly
   * that, and it is the reason this method exists rather than letting the write run the shuffle.
   *
   * `submitShuffleJob` runs the map stage and nothing downstream of it.
   *
   * The returned RDD comes from the top of the plan, not from the exchange node, so anything the
   * plan does above the exchange survives. Whether the caller may release its inputs turns on the
   * exchange covering every leaf: only then has all of the plan's input necessarily run to produce
   * the map output. That is checked rather than assumed - a plan with a leaf outside the exchange's
   * subtree takes the same branch as a plan with no exchange at all, and releases nothing.
   * Re-executing the whole plan does not re-run the shuffle: the scheduler recognises the map stage
   * as already complete and skips straight to reading its output. `SnowplowInternalSparkBridgeSpec`
   * pins that - its examples release the staged blocks and then read the result, which could only
   * work if the map stage is not recomputed.
   *
   * The id comes from the `MapOutputStatistics` the future yields rather than from
   * `exchange.shuffleId()`, so it is only in hand once the map stage is confirmed complete.
   *
   * A null `MapOutputStatistics` is possible, so the id is read defensively. Spark short-circuits
   * the shuffle when the plan's input RDD has no partitions and completes the future with null
   * rather than failing. `None` is the safe degradation, the same one a plan with no exchange gets
   * below; reading `shuffleId` off a null would NPE inside the commit's retry loop and surface as a
   * misleading `FailedToCommitToLake` alert. Nothing exercises this: it is unreachable while
   * `Processing.finalizeWindow` gates on `state.numEvents > 0` and every staged batch is one
   * partition. But that invariant lives two files away and this method does not enforce it, so the
   * guard is worth keeping whether or not a later Spark short-circuits the same way.
   *
   * A plan with no exchange returns its argument and `None`: there is no boundary to create, so
   * that window's staged batches live until it ends. Two things reach that branch, and only one of
   * them is expected: `prepareFinalDataFrame` coalescing instead of repartitioning when
   * `writerParallelism` is 1, and adaptive execution being switched on. AQE wraps the plan in an
   * `AdaptiveSparkPlanExec` that keeps its children out of a tree search like the one below, so no
   * exchange is found and this method degrades to a no-op - safely, because the caller keys the
   * release off the returned id, but silently. `reference.conf` disables AQE and the specs match
   * it, so nothing here exercises the AQE path; the `spark.conf` map reaches Spark without
   * validation, so a deployment that switches AQE back on loses this optimisation with no error.
   *
   * `ThreadUtils.awaitResult` rather than a bare `Await`: it is what Spark uses internally, so it
   * handles fatal-exception unwrapping and the blocking-context annotation consistently with the
   * rest of the session. Waiting without a timeout is deliberate. The future completes when the map
   * stage does, whether it succeeds, fails or is cancelled, so a timeout could only fire while the
   * job was still running - and there is nothing useful to do with that, since the await holds no
   * handle to cancel the job and a retried `prepareCommit` would submit a second shuffle for the
   * same window alongside the first. `Writer.write` blocks on Spark the same way.
   */
  def materializeShuffle(spark: SparkSession, df: DataFrame): (DataFrame, Option[MaterializedShuffle]) = {
    val queryExecution = df.queryExecution
    val plan           = queryExecution.executedPlan
    plan.collectFirst { case exchange: ShuffleExchangeLike => exchange } match {
      case Some(exchange) if exchange.collectLeaves() == plan.collectLeaves() =>
        val statistics = ThreadUtils.awaitResult(exchange.submitShuffleJob(), Duration.Inf)
        // The RDD below is this executed plan's output, so the executed plan's schema is the one
        // that describes it. Reaching for `df.schema` would be labelling those rows with a schema
        // taken from somewhere else.
        //
        // No `map(_.copy())`, unlike `Dataset.checkpoint` over the same RDD: `internalCreateDataFrame`
        // plans this as `RDDScanExec`, which re-projects each row, so the rows the shuffle reader
        // reuses never reach a consumer that buffers them.
        val relabelled = fromInternalRows(spark, queryExecution.toRdd, plan.schema)
        (relabelled, Option(statistics).map(s => MaterializedShuffle(s.shuffleId, s.bytesByPartitionId.sum)))
      case _ =>
        (df, None)
    }
  }

  /**
   * Releases the shuffle files behind a shuffle id returned by [[materializeShuffle]].
   *
   * Routed through `ContextCleaner.doCleanupShuffle` rather than unregistering from the
   * `MapOutputTracker` and removing the blocks separately, so the loader stays on the one path
   * Spark itself uses, `CleanerListener` notification included.
   *
   * Non-blocking, for the same reason `releaseCheckpointBlocks` is: the next window is already
   * accumulating by the time this runs. Only the file deletion is deferred - the shuffle is
   * unregistered before this returns, which `SnowplowInternalSparkBridgeSpec` pins by asserting on
   * the tracker immediately afterwards. If a Spark upgrade defers that too, that example fails
   * rather than the behaviour changing quietly.
   *
   * `cleaner` is `None` when `spark.cleaner.referenceTracking` is disabled, which the loader never
   * does. Nothing is released in that case, leaving the shuffle to be collected when the driver
   * drops its last reference.
   */
  def releaseShuffle(spark: SparkSession, shuffleId: Int): Unit =
    spark.sparkContext.cleaner.foreach(_.doCleanupShuffle(shuffleId, blocking = false))

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

  /**
   * The block manager's aggregate occupancy, from `BlockManagerMaster.getStorageStatus`
   * (`private[spark]`) - the same source Spark's own `BlockManagerSource` gauges read. In local
   * mode there is one block manager, so its aggregate is the whole loader's.
   *
   * `memUsed` is `onHeapMemUsed + offHeapMemUsed`, so it is the storage side of *both* pools added
   * together rather than whichever one is configured: with off-heap staging enabled it counts the
   * staged blocks in the off-heap pool plus Spark's own bookkeeping blocks on the heap. Those are
   * small here - task binaries are broadcast and a batch travels in its partition rather than the
   * broadcast - but it means the figure is not bounded by `spark.memory.offHeap.size` alone.
   *
   * `diskUsed` is every block the master has on disk, whoever put it there. Both storage levels
   * `checkpointedDataFrame` uses have `useDisk = true`, so a block whose memory put could not be
   * satisfied goes straight to disk without having been in memory first - resident on disk rather
   * than strictly evicted.
   *
   * The aggregate rather than `diskUsedByRdd` over the loader's own checkpoint RDDs, because what
   * the loader runs short of is the whole area, not its own share of it.
   *
   * Counts only blocks the master knows about, so shuffle files are absent - see
   * `MaterializedShuffle`.
   *
   * `getStorageStatus` is a blocking RPC ask rather than a field read, and the handler walks every
   * block into a `StorageStatus` per block manager. `BlockManagerMasterEndpoint` is an
   * `IsolatedThreadSafeRpcEndpoint`, and `BlockManagerMaster.updateBlockInfo` is an ask to that
   * same endpoint, so this O(blocks) work occupies the one inbox each staged batch's block
   * registration also passes through. Not the thread serialising task launches, but on the handover
   * path, and O(blocks) each time it runs. Called once per window.
   *
   * `askSync` waits up to `spark.rpc.askTimeout` (120s by default) rather than failing fast. The
   * caller is holding the window's staged blocks while it waits, and on the `LakeWriter` path it
   * also sits ahead of the release, so a wedged endpoint pins a window's memory area for that long
   * rather than raising.
   */
  def blockManagerUsage(spark: SparkSession): (Long, Long) = {
    val statuses = spark.sparkContext.env.blockManager.master.getStorageStatus
    (statuses.map(_.memUsed).sum, statuses.map(_.diskUsed).sum)
  }

  /**
   * The total size of every file in the block manager's directories under `spark.local.dir`.
   *
   * `DiskBlockManager.getAllFiles` (`private[spark]`) lists the hashed subdirectories of
   * `blockmgr-<uuid>` and nothing else, so unlike the two measures above this counts shuffle data
   * and index files, blocks on disk, checkpoint blocks and the spill files of the write's sort
   * together - the sorter takes those from the block manager too, so they land in the same
   * directories. But it is not everything on the filesystem. Anything else the loader or the JVM
   * writes to `spark.local.dir`, such as an event log or a JFR recording, is invisible to it.
   *
   * Stats every file it finds, so it is disk I/O on the same filesystem the commit shuffles and
   * sorts to. Files can be deleted while it runs; `File.length` returns 0 for a file that has gone
   * rather than failing, so the figure can be a slight under-count rather than an error.
   */
  def localDiskBytes(spark: SparkSession): Long =
    spark.sparkContext.env.blockManager.diskBlockManager.getAllFiles().map(_.length).sum
}
