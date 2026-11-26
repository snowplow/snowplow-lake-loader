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

import org.apache.spark.{MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.storage.RDDBlockId

/**
 * Test-only window onto a persisted block: the level it was put with, and where its bytes actually
 * live. Lives in org.apache.spark.sql, like the bridge, because `BlockManager`, `RDDBlockId` and
 * `BlockStatus` are all private[spark].
 *
 * Prefer this to `SparkContext.getRDDStorageInfo`, which reads `AppStatusStore` - filled
 * asynchronously by `AppStatusListener` off the `LiveListenerBus` and throttled by
 * `spark.ui.liveUpdate.period` (100ms). Reading that immediately after a write is a race.
 * `BlockManager.getStatus` reads the authoritative state, synchronously.
 */
object SnowplowSparkBlockProbe {

  /**
   * One partition of a persisted RDD. Flattened so callers need no private types.
   *
   * `useOffHeap`, `useDisk` and `deserialized` come from `BlockInfo.level`, which is the level the
   * block was *put* with - `getCurrentBlockStatus` is the one that rewrites those to reflect
   * residency. Only `memSize` and `diskSize` say where the bytes actually are.
   */
  final case class PersistedBlock(
    rddId: Int,
    useOffHeap: Boolean,
    useDisk: Boolean,
    deserialized: Boolean,
    memSize: Long,
    diskSize: Long
  )

  /** Partition 0 of every RDD this session currently has persisted. */
  def persistedBlocks(spark: SparkSession): List[PersistedBlock] = {
    val blockManager = SparkEnv.get.blockManager
    spark.sparkContext.getPersistentRDDs.keys.toList.sorted.flatMap { rddId =>
      blockManager.getStatus(RDDBlockId(rddId, 0)).map { status =>
        PersistedBlock(
          rddId,
          status.storageLevel.useOffHeap,
          status.storageLevel.useDisk,
          status.storageLevel.deserialized,
          status.memSize,
          status.diskSize
        )
      }
    }
  }

  /**
   * Whether the map output tracker still holds this shuffle.
   *
   * `SparkEnv.mapOutputTracker` is typed as the base `MapOutputTracker`, which exposes only
   * `unregisterShuffle`; `containsShuffle` is on `MapOutputTrackerMaster`, which is
   * `private[spark]` and so unreachable from the specs that need it. Local mode always constructs
   * that subclass.
   */
  def shuffleRegistered(shuffleId: Int): Boolean =
    SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].containsShuffle(shuffleId)

  /** Every shuffle the map output tracker currently holds, for diffing around a call. */
  def registeredShuffleIds: Set[Int] =
    SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].shuffleStatuses.keySet.toSet

  /**
   * Records the largest `peakExecutionMemory` of any task that writes records and ends from now
   * until `stop`, and how many such tasks there were.
   *
   * A sort inside a write reaches no `QueryExecution`: `FileFormatWriter` wraps the plan in its own
   * `SortExec` as it executes, so no plan a listener can read shows it. Its `UnsafeExternalSorter`
   * does claim execution memory, which a write that only reads a shuffle and writes files does not,
   * so this is what tells a sorted commit from an unsorted one.
   *
   * Only tasks whose `outputMetrics.recordsWritten` is non-zero count. A table write runs other
   * jobs around the one that writes files - Delta replays its log after committing, for one - and
   * those claim execution memory of their own, so counting every task makes the reading depend on
   * which of them end while the probe is listening.
   */
  final class PeakTaskExecutionMemory private[SnowplowSparkBlockProbe] (spark: SparkSession) {
    private val max        = new java.util.concurrent.atomic.AtomicLong(0L)
    private val writeTasks = new java.util.concurrent.atomic.AtomicInteger(0)
    private val listener = new org.apache.spark.scheduler.SparkListener {
      override def onTaskEnd(taskEnd: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit =
        Option(taskEnd.taskMetrics).filter(_.outputMetrics.recordsWritten > 0).foreach { m =>
          writeTasks.incrementAndGet()
          max.accumulateAndGet(m.peakExecutionMemory, (a, b) => math.max(a, b))
        }
    }
    spark.sparkContext.listenerBus.waitUntilEmpty()
    spark.sparkContext.addSparkListener(listener)

    /** Waits for the listener bus, which delivers task ends asynchronously. */
    def peak: Long = {
      spark.sparkContext.listenerBus.waitUntilEmpty()
      max.get
    }

    /** How many tasks wrote records, so a reading of 0 can be told from a write that was missed. */
    def tasksWritingRecords: Int = {
      spark.sparkContext.listenerBus.waitUntilEmpty()
      writeTasks.get
    }

    def stop(): Unit = spark.sparkContext.removeSparkListener(listener)
  }

  def trackPeakTaskExecutionMemory(spark: SparkSession): PeakTaskExecutionMemory =
    new PeakTaskExecutionMemory(spark)
}
