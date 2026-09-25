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

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.specs2.CatsEffect
import org.specs2.Specification

import org.apache.spark.rdd.{LocalRDDCheckpointData, ParallelCollectionRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import scala.concurrent.duration.DurationInt

// Lives in org.apache.spark.sql, like the bridge it tests, because the Spark internals these
// assertions reach for are private[spark].
class SnowplowInternalSparkBridgeSpec extends Specification with CatsEffect {
  import SnowplowInternalSparkBridgeSpec._

  override val Timeout = 60.seconds

  def is = sequential ^ s2"""
  SnowplowInternalSparkBridge.checkpointedDataFrame should:
    Leave the checkpointed RDD holding an off-heap storage level $e1
    Truncate the lineage so the batch's ParallelCollectionRDD is unreachable, which a bare persist would not $e2
    Return the rows it was given $e3
  Spark's own localCheckpoint, for comparison:
    Still silently discard useOffHeap, which is why the bridge exists $e4
  Both staging levels should:
    Reach the block manager as the level they asked for, useOffHeap included $e5
  """

  // Dataset.localCheckpoint(eager, OFF_HEAP) leaves useOffHeap false here. Asserting the level
  // rather than where the bytes landed keeps this independent of the pool size.
  def e1 = withSpark.use { spark =>
    IO.blocking {
      val rdd = checkpointedRdd(spark, batchOf(spark, "v1", "v2"))
      rdd.getStorageLevel.useOffHeap must beTrue
    }
  }

  // A bare persist would look equivalent but would leave the ParallelCollectionRDD - which holds
  // the whole batch of rows in a field - reachable from the accumulated view's plan all window.
  def e2 = withSpark.use { spark =>
    IO.blocking {
      val checkpointed = checkpointedRdd(spark, batchOf(spark, "v1", "v2"))
      // The same RDD shape without the checkpoint, so the assertion below is known to be falsifiable.
      val notCheckpointed = batchOf(spark, "v1", "v2").mapPartitions(identity)

      (retainsBatch(notCheckpointed) must beTrue) and (retainsBatch(checkpointed) must beFalse)
    }
  }

  def e3 = withSpark.use { spark =>
    IO.blocking {
      val (df, _) = checkpointed(spark, batchOf(spark, "v1", "v2"))
      df.collect().toList.map(_.getString(0)) must containTheSameElementsAs(List("v1", "v2"))
    }
  }

  // Reads both levels the bridge picks back from the block manager, rather than off the level we
  // asked for. `BlockManager.getStatus` reports the level a block was *put* with, not where its
  // bytes ended up, which is what this wants: the property is that `persist` did not rewrite the
  // level, and it holds whether or not the block fitted in the pool. It proves nothing about
  // residency - `memSize`/`diskSize` are those fields, and `SparkUtilsSpec` e16 asserts them.
  //
  // Note what this can and cannot fail on. `useDisk` is also checked by an `assume` inside
  // `LocalRDDCheckpointData.doCheckpoint`, which the bridge calls, so a level without a disk
  // fallback throws there and no block ever exists for this to inspect - asserting it here is a
  // restatement, not independent cover. `useOffHeap` is the half Spark checks nowhere: it is what
  // `transformStorageLevel` silently drops, and so the only one of the two this example can
  // actually catch. That is the bug the bridge exists for, which is why it is worth pinning on the
  // level the block manager recorded rather than on the argument we passed.
  def e5 = withSpark.use { spark =>
    IO.blocking {
      def blockFor(offHeap: Boolean) = {
        val (_, rdd) = SnowplowInternalSparkBridge.checkpointedDataFrame(spark, batchOf(spark, "v1", "v2"), schema, offHeap)
        val block    = SnowplowSparkBlockProbe.persistedBlocks(spark).find(_.rddId == rdd.id)
        SnowplowInternalSparkBridge.releaseCheckpointBlocks(rdd)
        block
      }

      val heap = blockFor(offHeap = false)
      val off  = blockFor(offHeap = true)

      (heap.map(_.useDisk) must beSome(true))
        .and(heap.map(_.useOffHeap) must beSome(false))
        .and(off.map(_.useDisk) must beSome(true))
        .and(off.map(_.useOffHeap) must beSome(true))
    }
  }

  // A canary on the specific mechanism, not on the symptom: Spark could instead fix this by making
  // Dataset.localCheckpoint bypass the normalisation and leave transformStorageLevel as it is, and
  // this would still pass. So when it fails, the bridge is definitely unnecessary; while it passes,
  // check localCheckpoint itself before assuming the bridge is still earning its keep.
  def e4 = {
    val transformed = LocalRDDCheckpointData.transformStorageLevel(StorageLevel.OFF_HEAP)
    (StorageLevel.OFF_HEAP.useOffHeap must beTrue) and (transformed.useOffHeap must beFalse)
  }
}

object SnowplowInternalSparkBridgeSpec {

  private val schema = StructType(Array(StructField("col_a", StringType, nullable = false)))

  private def batchOf(spark: SparkSession, values: String*): RDD[InternalRow] = {
    val rows = values.toList.map(v => InternalRow(UTF8String.fromString(v)): InternalRow)
    spark.sparkContext.parallelize(rows, 1)
  }

  private def checkpointed(spark: SparkSession, rows: RDD[InternalRow]): (DataFrame, RDD[InternalRow]) =
    SnowplowInternalSparkBridge.checkpointedDataFrame(spark, rows, schema, offHeap = true)

  /**
   * The RDD the bridge actually checkpointed, which it now hands back rather than hiding in a plan.
   */
  private def checkpointedRdd(spark: SparkSession, rows: RDD[InternalRow]): RDD[InternalRow] =
    checkpointed(spark, rows)._2

  /** Whether the ParallelCollectionRDD holding the batch's rows is still reachable from `rdd`. */
  private def retainsBatch(rdd: RDD[_]): Boolean =
    reachableFrom(rdd).exists(_.isInstanceOf[ParallelCollectionRDD[_]])

  /** Every RDD still reachable through the dependency chain. */
  private def reachableFrom(rdd: RDD[_]): List[RDD[_]] =
    rdd :: rdd.dependencies.toList.flatMap(d => reachableFrom(d.rdd))

  private def withSpark: Resource[IO, SparkSession] = {
    val build = IO.blocking(
      SparkSession
        .builder()
        .master("local")
        .appName("SnowplowInternalSparkBridgeSpec")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", 256L * 1024 * 1024)
        .getOrCreate()
    )
    Resource.make(build)(s => IO.blocking(s.close()))
  }
}
