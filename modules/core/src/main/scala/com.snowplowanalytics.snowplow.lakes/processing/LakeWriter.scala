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

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Mutex
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SnowplowInternalSparkBridge.MaterializedShuffle
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.lakes.{Alert, Config, DestinationSetupErrorCheck, RuntimeService}
import com.snowplowanalytics.snowplow.lakes.tables.{DeltaWriter, IcebergWriter, Writer}

trait LakeWriter[F[_]] {

  def createTable: F[Unit]

  /**
   * Creates an empty DataFrame with the atomic schema. Saves it with spark as a "view" so we can
   * refer to it by name later.
   *
   * Each window of events has its own DataFrame with unique name.
   */
  def initializeLocalDataFrame(viewName: String): F[Unit]

  /**
   * Append rows to the local DataFrame we are accumulating for this window
   *
   * The batch is encoded and checkpointed eagerly, so this runs work per call. It is only the union
   * with the accumulated view that stays lazy, and that is evaluated at the end of the window when
   * we commit to the lake.
   *
   * @param viewName
   *   Spark view for this window. The view should already be initialized before calling
   *   `localAppendRows`.
   * @param rows
   *   The new rows to append
   * @param schema
   *   The schema for this batch of rows. This might include new entities that are not already in
   *   the existing DataFrame.
   */
  def localAppendRows(
    viewName: String,
    rows: NonEmptyList[Row],
    schema: StructType
  ): F[Unit]

  /**
   * Un-saves the Spark view
   *
   * This allows Spark to clean up space when we are finished using a DataFrame. This must be called
   * at the end of each window.
   */
  def removeDataFrameFromDisk(viewName: String): F[Unit]

  /**
   * Repartition the window's events and materialize the shuffle, then release the staged batches.
   *
   * Replaces the window's view with the shuffled result, so `commit` writes from shuffle files
   * rather than from the staged batches. Those stop being needed the moment the shuffle map stage
   * completes, which is far short of the end of the commit - the rest of it is the write's sort,
   * the parquet encoding and the upload to the lake. Releasing here rather than at the end is what
   * keeps a window's blocks out of the memory area while its own commit is sorting.
   *
   * They also stop being recomputable at that point, which is the part worth knowing: the shuffle
   * output becomes the window's only copy of its data, so losing it fails the window rather than
   * being recovered by recomputing the map stage.
   *
   * Must be called before `commit`, and only once the window has stopped accepting appends.
   *
   * @param eventNameCounts
   *   The window's event-name histogram, keyed by the column's value so that `None` is the events
   *   with no `event_name`. Decides how the repartition spreads the window over the writer's
   *   partitions. See `WriterPartitioner`.
   */
  def prepareCommit(viewName: String, eventNameCounts: Map[Option[String], Int]): F[Unit]

  /**
   * Commit the window by writing it into the lake.
   *
   * Reads back the view `prepareCommit` left, which must therefore have run for this window: a
   * `commit` without it writes rows that were never repartitioned. Assigns `load_tstamp` as it
   * reads, so a retried commit stamps its rows with the time of the attempt that succeeds rather
   * than the time the window was prepared.
   */
  def commit(viewName: String): F[Unit]

  /** Get the total number of active data files in the table */
  def getTableDataFilesTotal: F[Option[Long]]

  /** Get the total number of table snapshots/versions currently retained in the transaction log */
  def getTableSnapshotsRetained: F[Option[Long]]

  /**
   * The bytes of `spark.local.dir` occupied by the shuffles of the windows currently open.
   *
   * Event-driven rather than sampled: each figure comes from the shuffle's own map-stage statistics
   * as the shuffle appears and is dropped when it is released, so this steps at most twice per
   * window. Nothing has to be timed to catch its peak. The figures themselves are upper bounds, not
   * measurements - `SnowplowInternalSparkBridge.MaterializedShuffle` says by how much.
   *
   * A window's shuffle is released at the end of its commit and the next window's is created at the
   * end of the next window, so two are only ever live at once when a commit has overrun a window -
   * the condition CLAUDE.md's "Windows overlap" section is about. Above one window's worth is
   * therefore a direct reading of that invariant.
   *
   * That reading is only available where `writerParallelism` is above 1. Below it
   * `prepareFinalDataFrame` coalesces, there is no exchange and no shuffle to account for, so this
   * is a constant 0 and the overrun reads identically to health.
   */
  def getShuffleDiskBytes: F[Long]

  /**
   * What the block manager held, in memory and on disk, when this window gave up its staged
   * batches.
   *
   * That is where a window's block occupancy peaks: every batch has been staged, and the next
   * window has been accumulating alongside since this one closed. Which call takes it depends on
   * whether the window had a shuffle to hand over to - `prepareCommit` where it did,
   * `recordBlockManagerPeak` where it did not.
   *
   * Those are not the same point in the cycle, so readings are not comparable across core counts.
   * `prepareCommit` runs at window end, so the reading there includes only what the next window
   * accumulated during the shuffle; `recordBlockManagerPeak` runs from the bracket finalizer, after
   * the whole commit, so it includes a commit's worth more. A figure taken on a two-core deployment
   * will therefore read higher than the same occupancy on an eight-core one, which matters if
   * `spark.memory.offHeap.size` is being sized from it.
   *
   * The memory half is `onHeapMemUsed + offHeapMemUsed` and the disk half is every block the master
   * has on disk however it got there - `SnowplowInternalSparkBridge.blockManagerUsage` has the
   * consequences of both. Both halves are now serialized, compressed bytes, so the memory half
   * counts the same units as the disk half; readings from before the heap path stored batches
   * serialized counted live objects there instead, and are larger for the same events.
   *
   * `None` for a window that never staged anything, and for one that has not got that far yet. The
   * measurement lives in that window's own record, so it is taken once per window and cannot be
   * displaced by another window taking its own - which under the overlap CLAUDE.md's "Windows
   * overlap" section describes would otherwise drop one of the two readings.
   *
   * One value rather than two accessors, because the disk figure only means anything against the
   * memory one: together they give the split, and a pair read separately could come from two
   * different measurements.
   *
   * That split is what sizing the area moves, and it is not a reading of whether the window fit.
   * `spark.memory.storageFraction` is 0 and `Writer.write`'s sort draws a window's rows from the
   * same area, so the previous window's sort - running while this one stages - empties it every
   * window whatever it is sized at. The memory half is what was still resident at the point above
   * and the disk half is the rest, so a large disk half is the normal condition rather than a
   * symptom; what moves the ratio is how much area was left for this window to keep.
   *
   * The peak of *block* occupancy, not of the memory area as a whole. The area also holds execution
   * memory: the shuffle's, which peaks inside the call this measurement is taken after, and the
   * write's sort, which is the larger claim and peaks later still. Nothing the loader samples
   * itself can see either; Spark's own `ExecutorMetrics` peaks are the instrument for that.
   */
  def getStorageUsage(viewName: String): F[Option[LakeWriter.BlockManagerUsage]]

  /**
   * The total size of the block manager's directories under `spark.local.dir`.
   *
   * The only one of the three that measures files rather than the loader's own account of what it
   * put there, and so the only one to compare against a storage limit - at one reading per window,
   * taken near the top of the sawtooth this quantity traces as a window's shuffle appears and the
   * next window's evictions accumulate. A per-window peak rather than a live level, so it will not
   * show a single window filling the disk before that window ends. Covers the blocks on disk, the
   * checkpoint blocks, the shuffles and, while a commit is sorting, that sort's spill files
   * together, but only within the block manager's own directories: it is not a reading of the whole
   * filesystem, and `SnowplowInternalSparkBridge.localDiskBytes` says what it misses.
   *
   * Not a total that `getShuffleDiskBytes` and `getStorageUsage` decompose, and not disjoint from
   * them either: a disk-resident block's file is counted here and in `getStorageUsage`'s disk half.
   * Where a window had a shuffle, this counts that shuffle plus whatever the *next* window has put
   * on disk so far, while the storage figures are a peak from before this window released its own
   * blocks - three quantities from two moments, which no subtraction reconciles. What it does give
   * is files neither of the others can account for, qualitatively rather than by arithmetic.
   *
   * Disk I/O, on the filesystem the commit shuffles and sorts to.
   *
   * `None` means the read failed, not that the figure is unavailable - unlike the table metrics
   * above, whose `None` is the table format declining to report one, this figure always exists. It
   * is optional only so `withHandledErrors` can drop a failed read instead of failing the window
   * over a metric, which leaves the gauge holding its last good value rather than replacing it with
   * a 0 that reads as plenty of free disk. A push-backed gauge starts at 0 regardless, so this
   * preserves a reading once there has been one but cannot express "never measured".
   */
  def getDiskBytes: F[Option[Long]]

  /**
   * Measures the window's block occupancy, if this window still holds the batches it staged.
   *
   * Keyed on the batches rather than on the absence of a shuffle, which is not the same test. A
   * window whose `prepareCommit` found a shuffle has none left - that call released them and
   * measured them on the way - and a window that staged nothing never had any, so measuring for
   * either would report the *next* window's first moments and overwrite a real peak with them. What
   * is left is `writerParallelism` of 1, where nothing was released because there was no shuffle to
   * hand over to, and a window whose `prepareCommit` failed. Both still hold their batches, and for
   * both this is the last moment before they go.
   *
   * Must be called before `removeDataFrameFromDisk` for the window, which discards its record, and
   * its result is what `getStorageUsage` then reports for that window.
   */
  def recordBlockManagerPeak(viewName: String): F[Unit]
}

object LakeWriter {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  trait WithHandledErrors[F[_]] extends LakeWriter[F]

  /** What the block manager is holding, in bytes, split by where it is holding it. */
  final case class BlockManagerUsage(memoryBytes: Long, diskBytes: Long)

  /**
   * @param cores
   *   How many task slots Spark gets, and what `writerParallelism` is derived from. A parameter
   *   rather than a reading of `availableProcessors` so that the two cannot diverge, and so that a
   *   spec can exercise both the shuffling and the coalescing plan on any machine. Must be at least
   *   1: `chooseWriterParallelism` floors, but `local[0, n]` would reach Spark as it stands and
   *   fail opaquely.
   */
  def build[F[_]: Async](
    config: Config.Spark,
    target: Config.Target,
    respectIgluNullability: Boolean,
    cores: Int
  ): Resource[F, LakeWriter[F]] = {
    val w = target match {
      case c: Config.Delta   => new DeltaWriter(c)
      case c: Config.Iceberg => new IcebergWriter(c)
    }
    // Delta writes all inner struct fields as nullable anyway, so correction is a no-op there.
    // If respectIgluNullability is disabled, all fields are nullable by schema design, so no correction needed.
    val shouldRestoreNullability = respectIgluNullability && !target.isInstanceOf[Config.Delta]
    for {
      session <- SparkUtils.session[F](config, w, target, cores)
      stageOffHeap <- Resource.eval(SparkUtils.stageBatchesOffHeap[F](session))
      writerParallelism = chooseWriterParallelism(cores)
      mutex1 <- Resource.eval(Mutex[F])
      mutex2 <- Resource.eval(Mutex[F])
      windowResources <- Resource.eval(Ref[F].of(Map.empty[String, WindowResources]))
    } yield impl(
      session,
      w,
      writerParallelism,
      config.writerPartitioning,
      shouldRestoreNullability,
      stageOffHeap,
      mutex1,
      mutex2,
      windowResources
    )
  }

  def withHandledErrors[F[_]: Async](
    underlying: LakeWriter[F],
    appHealth: AppHealth.Interface[F, Alert, RuntimeService],
    retries: Config.Retries,
    destinationSetupErrorCheck: DestinationSetupErrorCheck
  ): WithHandledErrors[F] = new WithHandledErrors[F] {
    def createTable: F[Unit] =
      Retrying.withRetries(
        appHealth,
        retries.transientErrors,
        retries.setupErrors,
        RuntimeService.SparkWriter,
        Alert.FailedToCreateEventsTable,
        destinationSetupErrorCheck
      ) { _ =>
        underlying.createTable
      } <* appHealth.beHealthyForSetup

    def initializeLocalDataFrame(viewName: String): F[Unit] =
      underlying.initializeLocalDataFrame(viewName)

    def localAppendRows(
      viewName: String,
      rows: NonEmptyList[Row],
      schema: StructType
    ): F[Unit] =
      underlying.localAppendRows(viewName, rows, schema)

    def removeDataFrameFromDisk(viewName: String): F[Unit] =
      underlying.removeDataFrameFromDisk(viewName)

    // Nothing `prepareCommit` does touches the lake, so nothing it raises can be triaged as a
    // customer setup error. Wrapping it would alert customers about failures they cannot act on,
    // and its success would clear the setup-healthy flag on no evidence about the destination at
    // all - hence unwrapped, unlike `commit` below.
    // Dropping the retries with it is deliberate rather than a side effect. Spark has already
    // retried the shuffle's tasks `spark.taskRetries` times, and a retry here would resubmit the
    // whole map stage rather than the task that failed. Nothing is acked before `commit`, so the
    // window is re-consumed on restart.
    def prepareCommit(viewName: String, eventNameCounts: Map[Option[String], Int]): F[Unit] =
      underlying.prepareCommit(viewName, eventNameCounts)

    def commit(viewName: String): F[Unit] =
      Retrying.withRetries(
        appHealth,
        retries.transientErrors,
        retries.setupErrors,
        RuntimeService.SparkWriter,
        Alert.FailedToCommitToLake,
        destinationSetupErrorCheck
      ) { _ =>
        underlying.commit(viewName)
      } <* appHealth.beHealthyForSetup

    def getTableDataFilesTotal: F[Option[Long]] =
      underlying.getTableDataFilesTotal.handleErrorWith { e =>
        Logger[F].warn(e)("Failed to get table_data_files_total metric").as(None)
      }

    def getTableSnapshotsRetained: F[Option[Long]] =
      underlying.getTableSnapshotsRetained.handleErrorWith { e =>
        Logger[F].warn(e)("Failed to get table_snapshots_retained metric").as(None)
      }

    // `getDiskBytes` is the only one wrapped here. `getShuffleDiskBytes` and `getStorageUsage`
    // read state the underlying writer already holds, and `recordBlockManagerPeak` does reach Spark
    // but handles its own errors in `recordPeak`, because `prepareCommit` calls it too.
    def getShuffleDiskBytes: F[Long] =
      underlying.getShuffleDiskBytes

    def getStorageUsage(viewName: String): F[Option[BlockManagerUsage]] =
      underlying.getStorageUsage(viewName)

    def getDiskBytes: F[Option[Long]] =
      underlying.getDiskBytes.handleErrorWith { e =>
        Logger[F].warn(e)("Failed to get spark_disk_bytes metric").as(None)
      }

    def recordBlockManagerPeak(viewName: String): F[Unit] =
      underlying.recordBlockManagerPeak(viewName)
  }

  /**
   * What a window has acquired inside Spark that must be released when it ends.
   *
   * Never both: `checkpointed` accumulates as batches are staged, and `prepareCommit` swaps it for
   * `shuffle` in one step. Which of the two holds the window's data depends on how far it got, so
   * `removeDataFrameFromDisk` attempts both.
   *
   * `blockManagerPeak` is not a resource to release - it is a measurement - but it lives here
   * because it is taken once per window, and holding it beside the window's other state is what
   * says so. `getStorageUsage` reads it back for the window that is ending.
   *
   * `shuffle` carries the shuffle's size on disk as well as its id, which is what
   * `getShuffleDiskBytes` sums. One entry per window, replaced atomically, so the gauge tracks the
   * shuffles the loader is holding ids for rather than every shuffle that exists: a second
   * `prepareCommit` for the same window would drop the first shuffle's bytes while its files are
   * still on disk awaiting the `ContextCleaner`. `getDiskBytes` is what covers that.
   */
  private final case class WindowResources(
    checkpointed: List[RDD[InternalRow]],
    shuffle: Option[MaterializedShuffle],
    blockManagerPeak: Option[BlockManagerUsage]
  ) {
    def staged(rdd: RDD[InternalRow]): WindowResources = copy(checkpointed = rdd :: checkpointed)

    /** Gives up the staged batches: the shuffle holds the window's data from here. */
    def shuffled(s: MaterializedShuffle): WindowResources = copy(checkpointed = Nil, shuffle = Some(s))
  }

  private object WindowResources {
    val empty: WindowResources = WindowResources(Nil, None, None)
  }

  /**
   * Implementation of the LakeWriter
   *
   * @param mutexForRemoteWriting
   *   This mutex is needed because we allow overlapping windows. It prevents two different windows
   *   from trying to run `w.write` - the remote write to Iceberg/Delta - at the same time. It
   *   deliberately does not cover `prepareCommit`'s repartition and shuffle. Holding it over those
   *   would leave the next window's `prepareCommit` queued behind this window's write with its
   *   staged blocks still resident, which is the cost the split exists to avoid. So under the lag
   *   condition described in CLAUDE.md's "Windows overlap" section, window N+1's shuffle can run
   *   concurrently with window N's write. Nothing is staging while that is true: at most two
   *   windows are open, and under lag those two are the ones finalizing.
   * @param mutexForLocalAppending
   *   This mutex is needed because `SparkUtils.appendStagedBatch` would otherwise have a race
   *   condition: It fetches a saved dataframe by name, modifies it, and re-saves the dataframe by
   *   the same name. It deliberately covers neither `SparkUtils.encodeBatch` nor
   *   `SparkUtils.stageBatch`, which are the expensive steps and never touch the named view.
   * @param windowResources
   *   What each open window has acquired inside Spark and must release when it ends: the checkpoint
   *   blocks of its staged batches, and, once `prepareCommit` has run, the shuffle holding the
   *   window's data instead. Released as soon as the window is dropped rather than whenever Spark's
   *   `ContextCleaner` next gets to them. Keyed by view name, and the entry is removed when the
   *   window ends. What keeps this map bounded is that `Processing.manageDataFrame` brackets each
   *   window, so `removeDataFrameFromDisk` always runs, and that fs2 joins the
   *   `parEvalMapUnordered` fibers before running that finalizer, so no append can add an entry
   *   back after its window removed one. An entry that outlived its window would hold a strong
   *   reference to its RDDs for the life of the app, defeating the ContextCleaner as well, which is
   *   worse than not tracking them at all.
   */
  private def impl[F[_]: Sync](
    spark: SparkSession,
    w: Writer,
    writerParallelism: Int,
    writerPartitioning: Config.WriterPartitioning,
    shouldRestoreNullability: Boolean,
    stageOffHeap: Boolean,
    mutexForRemoteWriting: Mutex[F],
    mutexForLocalAppending: Mutex[F],
    windowResources: Ref[F, Map[String, WindowResources]]
  ): LakeWriter[F] = new LakeWriter[F] {
    def createTable: F[Unit] =
      w.prepareTable(spark) *> logTableDescription

    /**
     * Swallows its errors rather than leaving them to `withHandledErrors`, which would retry
     * `createTable` and raise `FailedToCreateEventsTable` over a log line.
     */
    private def logTableDescription: F[Unit] =
      w.describeTable(spark)
        .flatMap(_.traverse_(Logger[F].info(_)))
        .handleErrorWith { e =>
          Logger[F].warn(e)("Could not read the table's properties for logging")
        }

    def initializeLocalDataFrame(viewName: String): F[Unit] =
      SparkUtils.initializeLocalDataFrame(spark, viewName)

    def localAppendRows(
      viewName: String,
      rows: NonEmptyList[Row],
      schema: StructType
    ): F[Unit] =
      for {
        // Encoding and staging are the expensive steps and need no exclusive access, so they stay
        // outside the mutex. Callers reach here from a `parEvalMapUnordered`, so batches encode and
        // stage in parallel and only the view read-modify-write is serialized.
        encoded <- SparkUtils.encodeBatch(spark, rows, schema)
        staged <- SparkUtils.stageBatch(spark, encoded, schema, stageOffHeap)
        // Recorded as soon as the batch is staged, which is when its blocks start existing, and
        // before the wait for the append mutex rather than after it. A batch cancelled between the
        // two stages blocks that never get recorded, and those are released by the ContextCleaner
        // exactly as they were before this map existed - the safe way to lose the race, and not
        // worth an `uncancelable` that would also make the staging job itself uninterruptible.
        _ <- windowResources.update { m =>
               val existing = m.getOrElse(viewName, WindowResources.empty)
               m.updated(viewName, existing.staged(staged.checkpointed))
             }
        _ <- mutexForLocalAppending.lock.surround {
               SparkUtils.appendStagedBatch(spark, viewName, staged.df, schema, shouldRestoreNullability)
             }
      } yield ()

    def removeDataFrameFromDisk(viewName: String) =
      for {
        resources <- windowResources.modify(m => (m - viewName, m.getOrElse(viewName, WindowResources.empty)))
        _ <- SparkUtils.dropView(spark, viewName, resources.checkpointed, resources.shuffle.map(_.id))
      } yield ()

    def prepareCommit(viewName: String, eventNameCounts: Map[Option[String], Int]): F[Unit] =
      for {
        df <- SparkUtils.prepareFinalDataFrame(spark, viewName, writerParallelism, writerPartitioning, eventNameCounts)
        shuffled <- SparkUtils.materializeShuffle(spark, df)
        // Only when there is a shuffle, because only then does the release below happen here. With
        // no shuffle the batches live on and `recordBlockManagerPeak` measures them at the drop.
        _ <- shuffled.shuffle.traverse_(_ => recordPeak(spark, viewName, windowResources))
        // Always, even with no shuffle: `commit` reads the view, so the repartition has to be what
        // is registered under this name.
        _ <- SparkUtils.replaceView(viewName, shuffled.df)
        // Only when there is a shuffle to hand the window over to. With no exchange in the plan -
        // `writerParallelism` of 1, so `prepareFinalDataFrame` coalesced - the view still reads
        // from the staged batches, and releasing them here would make it unreadable. That window
        // keeps them until `removeDataFrameFromDisk`.
        //
        // Do not put a fallible step between `replaceView` and this modify. A retry re-enters at the
        // top and shuffles again, and `WindowResources` holds one shuffle id, so the first shuffle
        // would be left with nothing holding its id.
        //
        // One modify, so the entry always holds either the blocks or the shuffle: whatever this
        // method achieved, `removeDataFrameFromDisk` can finish it.
        _ <- shuffled.shuffle.traverse_ { shuffle =>
               windowResources
                 .modify { m =>
                   val existing = m.getOrElse(viewName, WindowResources.empty)
                   (m.updated(viewName, existing.shuffled(shuffle)), existing.checkpointed)
                 }
                 .flatMap(SparkUtils.releaseStagedBatches[F])
             }
      } yield ()

    def commit(viewName: String): F[Unit] =
      SparkUtils.readFinalDataFrame(spark, viewName).flatMap { df =>
        mutexForRemoteWriting.lock.surround {
          w.write(df)
        }
      }

    def getTableDataFilesTotal: F[Option[Long]] =
      w.getTableDataFilesTotal(spark)

    def getTableSnapshotsRetained: F[Option[Long]] =
      w.getTableSnapshotsRetained(spark)

    def getShuffleDiskBytes: F[Long] =
      windowResources.get.map(_.values.flatMap(_.shuffle).map(_.diskBytes).sum)

    def getStorageUsage(viewName: String): F[Option[BlockManagerUsage]] =
      windowResources.get.map(_.get(viewName).flatMap(_.blockManagerPeak))

    def getDiskBytes: F[Option[Long]] =
      SparkUtils.localDiskBytes(spark).map(_.some)

    def recordBlockManagerPeak(viewName: String): F[Unit] =
      windowResources.get.flatMap { m =>
        if (m.get(viewName).exists(_.checkpointed.nonEmpty)) recordPeak(spark, viewName, windowResources)
        else Sync[F].unit
      }
  }

  /**
   * Measures the block manager and stores the result for `getStorageUsage` to report.
   *
   * Cannot raise: both callers are on a window's own path, where an error costs a restart and a
   * re-consumed window. The previous window's figure stays in place instead, which is stale rather
   * than wrong.
   */
  private def recordPeak[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    windowResources: Ref[F, Map[String, WindowResources]]
  ): F[Unit] =
    SparkUtils
      .blockManagerUsage(spark)
      .map { case (memoryBytes, diskBytes) => BlockManagerUsage(memoryBytes, diskBytes) }
      .flatMap { usage =>
        // Only into an entry that already exists: a window whose record has gone has ended, and
        // resurrecting it here would leave the map holding it for the life of the app.
        windowResources.update { m =>
          m.get(viewName).fold(m)(existing => m.updated(viewName, existing.copy(blockManagerPeak = Some(usage))))
        }
      }
      .handleErrorWith { e =>
        Logger[F].warn(e)("Could not measure the block manager's usage. Leaving those metrics at their previous values.")
      }

  /**
   * Allow spark to parallelize over _most_ of the available processors for writing to the lake,
   * because this speeds up how quickly we can sink a batch.
   *
   * But leave 1 processor always available, so that we are never blocked when trying to save one of
   * the intermediate dataframes.
   */
  private def chooseWriterParallelism(cores: Int): Int =
    (cores - 1).max(1)
}
