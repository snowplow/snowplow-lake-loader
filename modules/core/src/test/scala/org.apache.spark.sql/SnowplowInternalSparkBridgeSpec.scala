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
    Reach the block manager as the level they asked for, useOffHeap and deserialized included $e5
  SnowplowInternalSparkBridge.materializeShuffle should:
    Return the same rows as the DataFrame it was given $e6
    Leave the result readable after the staged blocks are released, unlike the original $e7
    Survive being read twice after that release, which is what a retried commit does $e8
    Return the DataFrame unchanged, and no shuffle id, when the plan has no exchange $e9
  SnowplowInternalSparkBridge.releaseShuffle should:
    Unregister the shuffle, instead of leaving it to the ContextCleaner $e10
  SnowplowInternalSparkBridge.blockManagerUsage should:
    Report the memory held by a staged batch $e11
  SnowplowInternalSparkBridge.localDiskBytes should:
    Count a shuffle's files, which the block manager's own disk accounting cannot see $e12
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
  // restatement, not independent cover. The other two are covered only here. `useOffHeap` is what
  // `transformStorageLevel` silently drops, which is the bug the bridge exists for and the reason
  // to read the level back off the block manager rather than off the argument we passed.
  // `deserialized` Spark leaves alone, but nothing else catches it either: staging batches as live
  // rows again would give correct results and pass every other spec, surfacing only as a
  // throughput regression.
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
        .and(heap.map(_.deserialized) must beSome(false))
        .and(off.map(_.useDisk) must beSome(true))
        .and(off.map(_.useOffHeap) must beSome(true))
        .and(off.map(_.deserialized) must beSome(false))
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

  // The rows must survive the round trip through shuffle files, not just the plan surgery.
  def e6 = withSpark.use { spark =>
    IO.blocking {
      val (staged, _)   = checkpointed(spark, batchOf(spark, "v1", "v2", "v3"))
      val (shuffled, _) = SnowplowInternalSparkBridge.materializeShuffle(spark, repartitioned(staged))
      shuffled.collect().toList.map(_.getString(0)) must containTheSameElementsAs(List("v1", "v2", "v3"))
    }
  }

  // The property the whole design rests on, with a control so it is falsifiable.
  //
  // The control must be a SEPARATE DataFrame over the same staged batch, not the one handed to
  // materializeShuffle. A Dataset caches its executed plan and RDD, so materializing through one
  // leaves the shuffled RDD memoized on that same object: re-collecting the argument afterwards
  // reuses it and never touches the checkpoint, whatever materializeShuffle did. A same-object
  // control therefore cannot fail and proves nothing.
  //
  // Asserting the shuffle id is Some matters too: with no exchange the method returns its argument
  // unchanged, and this example would then be releasing blocks the DataFrame still needs. Without
  // this assertion that shows up as a confusing failure rather than a clear one.
  def e7 = withSpark.use { spark =>
    IO.blocking {
      val (staged, rdd)         = checkpointed(spark, batchOf(spark, "v1", "v2", "v3"))
      val control               = repartitioned(staged)
      val (shuffled, shuffleId) = SnowplowInternalSparkBridge.materializeShuffle(spark, repartitioned(staged))
      SnowplowInternalSparkBridge.releaseCheckpointBlocks(rdd)

      (shuffleId must beSome)
        .and(shuffled.collect().toList.map(_.getString(0)) must containTheSameElementsAs(List("v1", "v2", "v3")))
        .and(control.collect() must throwA[Exception])
    }
  }

  // A retried commit re-reads the view and re-executes. Both reads must come from shuffle files.
  def e8 = withSpark.use { spark =>
    IO.blocking {
      val (staged, rdd)         = checkpointed(spark, batchOf(spark, "v1", "v2", "v3"))
      val (shuffled, shuffleId) = SnowplowInternalSparkBridge.materializeShuffle(spark, repartitioned(staged))
      SnowplowInternalSparkBridge.releaseCheckpointBlocks(rdd)

      val first  = shuffled.collect().toList.map(_.getString(0))
      val second = shuffled.collect().toList.map(_.getString(0))
      (shuffleId must beSome)
        .and(first must containTheSameElementsAs(List("v1", "v2", "v3")))
        .and(second must containTheSameElementsAs(first))
    }
  }

  // LakeWriter.chooseWriterParallelism can return 1, and prepareFinalDataFrame then coalesces
  // instead of repartitioning, so there is no exchange to materialize. That window must keep
  // today's behaviour rather than fail.
  def e9 = withSpark.use { spark =>
    IO.blocking {
      val (staged, _)           = checkpointed(spark, batchOf(spark, "v1", "v2"))
      val coalesced             = staged.coalesce(1)
      val (returned, shuffleId) = SnowplowInternalSparkBridge.materializeShuffle(spark, coalesced)

      (returned must beTheSameAs(coalesced)) and (shuffleId must beNone)
    }
  }

  // The window's view holds the ShuffleDependency alive for the length of the commit, so the
  // ContextCleaner's weak reference never fires while it matters and the loader has to unregister
  // the shuffle itself. Otherwise a window's worth of shuffle files - gigabytes, at production
  // volumes - accumulates on spark.local.dir, the same filesystem the staged blocks spill to.
  def e10 = withSpark.use { spark =>
    IO.blocking {
      val (staged, _)     = checkpointed(spark, batchOf(spark, "v1", "v2"))
      val (_, shuffleOpt) = SnowplowInternalSparkBridge.materializeShuffle(spark, repartitioned(staged))
      val shuffleId       = shuffleOpt.map(_.id).getOrElse(throw new IllegalStateException("Expected a shuffle id"))

      val registeredBefore = SnowplowSparkBlockProbe.shuffleRegistered(shuffleId)
      SnowplowInternalSparkBridge.releaseShuffle(spark, shuffleId)
      val registeredAfter = SnowplowSparkBlockProbe.shuffleRegistered(shuffleId)

      (registeredBefore must beTrue) and (registeredAfter must beFalse)
    }
  }

  // master.getStorageStatus is private[spark], so a Spark upgrade could move it out of reach. A
  // lower bound rather than a size, because the block manager also holds Spark's own broadcast
  // blocks and the figure is never just the batch.
  def e11 = withSpark.use { spark =>
    IO.blocking {
      val before = SnowplowInternalSparkBridge.blockManagerUsage(spark)._1
      val _      = checkpointed(spark, batchOf(spark, "v1", "v2"))
      val after  = SnowplowInternalSparkBridge.blockManagerUsage(spark)._1

      after must beGreaterThan(before)
    }
  }

  // Shuffle files are not blocks, so the block manager's own diskUsed cannot see them. Both halves
  // are asserted together because the metrics depend on that difference: a Spark release that began
  // registering shuffle blocks with the master would make this pair contradictory, and would also
  // make spark_disk_bytes and spark_storage_disk_bytes double-count.
  def e12 = withSpark.use { spark =>
    IO.blocking {
      val (staged, _) = checkpointed(spark, batchOf(spark, "v1", "v2"))

      val filesBefore  = SnowplowInternalSparkBridge.localDiskBytes(spark)
      val blocksBefore = SnowplowInternalSparkBridge.blockManagerUsage(spark)._2
      val (_, shuffle) = SnowplowInternalSparkBridge.materializeShuffle(spark, repartitioned(staged))
      val filesAfter   = SnowplowInternalSparkBridge.localDiskBytes(spark)
      val blocksAfter  = SnowplowInternalSparkBridge.blockManagerUsage(spark)._2

      val shuffleDiskBytes = shuffle.map(_.diskBytes).getOrElse(throw new IllegalStateException("Expected a shuffle"))

      // A diff across the shuffle rather than absolute values: what the metrics rely on is that the
      // shuffle moves one of these and not the other. Not localDiskBytes >= shuffleDiskBytes, which
      // is not an invariant - the shuffle figure is an upper bound overstating by up to 10%, more
      // than the index files localDiskBytes counts and it does not.
      //
      // The block figure is asserted to move by less than the shuffle, not by exactly zero:
      // storageFraction is 0 here to match reference.conf, so the shuffle acquiring execution
      // memory may legitimately evict the staged block to disk between the two reads. That would
      // move it by a batch, which is what makes the comparison against the shuffle's own size the
      // durable form of "the shuffle files are not in there".
      (shuffleDiskBytes must beGreaterThan(0L)) and
        (filesAfter must beGreaterThan(filesBefore)) and
        ((blocksAfter - blocksBefore) must beLessThan(shuffleDiskBytes))
    }
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

  /** A plan with exactly one exchange in it, of the kind `prepareFinalDataFrame` produces. */
  private def repartitioned(df: DataFrame): DataFrame =
    df.repartitionById(2, functions.pmod(functions.hash(functions.col("col_a")), functions.lit(2)))

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
        // Matches reference.conf, and load-bearing here: materializeShuffle degrades to a no-op
        // under AQE, which would leave every example below asserting nothing.
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", 256L * 1024 * 1024)
        .getOrCreate()
    )
    Resource.make(build)(s => IO.blocking(s.close()))
  }
}
