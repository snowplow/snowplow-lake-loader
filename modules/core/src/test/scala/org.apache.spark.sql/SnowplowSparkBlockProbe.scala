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

import org.apache.spark.SparkEnv
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
   * `useOffHeap` and `useDisk` come from `BlockInfo.level`, which is the level the block was *put*
   * with - `getCurrentBlockStatus` is the one that rewrites those to reflect residency. Only
   * `memSize` and `diskSize` say where the bytes actually are.
   */
  final case class PersistedBlock(
    rddId: Int,
    useOffHeap: Boolean,
    useDisk: Boolean,
    memSize: Long,
    diskSize: Long
  )

  /** Partition 0 of every RDD this session currently has persisted. */
  def persistedBlocks(spark: SparkSession): List[PersistedBlock] = {
    val blockManager = SparkEnv.get.blockManager
    spark.sparkContext.getPersistentRDDs.keys.toList.sorted.flatMap { rddId =>
      blockManager.getStatus(RDDBlockId(rddId, 0)).map { status =>
        PersistedBlock(rddId, status.storageLevel.useOffHeap, status.storageLevel.useDisk, status.memSize, status.diskSize)
      }
    }
  }
}
