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

import com.snowplowanalytics.snowplow.lakes.Config

/**
 * Decides how a window's events are spread over the Spark partitions that write to the lake.
 *
 * Three requirements pull against each other.
 *
 * The commit must use exactly `writerParallelism` partitions, and never more. That count is
 * `availableProcessors - 1`, so a commit of that many tasks leaves one Spark task slot free for the
 * per-batch handover from cats-effect into Spark. Spark never preempts a running task, so `pool1`'s
 * scheduling weight decides only which queued task goes next, not whether a slot exists at all.
 *
 * It must spread work evenly, because the heaviest partition is the commit's critical path, and a
 * commit that outlasts its window makes the loader fall progressively behind.
 *
 * And it must keep each `event_name` in as few partitions as possible, because the lake is
 * partitioned by `event_name`, so every extra partition holding a name is an extra, smaller file.
 *
 * `WindowState.eventNameCounts` gives every key's size on the driver before the shuffle runs, which
 * is what allows the assignment to be chosen outright rather than left to a hash:
 *
 *   - Keys are bin-packed into exactly `writerParallelism` bins, largest first, which is within 4/3
 *     of optimal in the worst case and much closer in practice. A hash cannot substitute:
 *     scattering keys over so few bins is lumpy, and hashing's only remedy is more partitions,
 *     which the reserved slot forbids.
 *   - `None` is a key like any other, because the lake has an `event_name=null` partition just as
 *     it has one per name. A window of entirely nameless events balances like one sharing a name.
 *   - A key bigger than a fair share fits in no bin, so it is cut into pieces first, and which
 *     piece a row belongs to is decided by hashing `event_id` - a UUID, so it splits evenly. This
 *     is the one place a hash is right, because here an even split is all that is wanted.
 *   - Packing is only as even as its largest item, and a key at or below a fair share is not cut,
 *     so a window of `writerParallelism + 1` similar keys would pack two whole keys into one
 *     partition. Two plans are therefore built - one cutting only what cannot fit a partition, one
 *     cutting everything the floor allows - and the second wins only if its heaviest partition is
 *     lighter by `WorthSplittingFor`, so a window is not fragmented for a gain that does not repay
 *     the output files it costs.
 *   - `splitsPerFairShare` sets the piece size aimed for, as a fraction of a fair share, and
 *     `minEventsPerSplit` caps the piece count so that no piece falls below it. A key with fewer
 *     than twice `minEventsPerSplit` events therefore cannot be split at all, and a window of a few
 *     such keys spreads unevenly with no setting to fix it - which is the trade that floor exists
 *     to make. Both are described on `Config.WriterPartitioning`.
 *
 * Balance is counted in events, not bytes, so a window mixing `page_ping`s with `page_view`s
 * carrying twenty entities each is even by count and skewed by write time.
 *
 * This object is deliberately free of Spark types: it decides the assignment, and
 * `SparkUtils.repartitionForWriting` imposes it on a DataFrame.
 */
private[processing] object WriterPartitioner {

  /**
   * How much lighter the split plan's heaviest partition must be before it is worth its extra
   * output files. Without a margin any improvement at all buys them, so a window would be cut up
   * for a fraction of a percent of balance it will never notice.
   */
  private val WorthSplittingFor = 0.98

  /**
   * @param numPartitions
   *   How many partitions to shuffle into. Always `writerParallelism`.
   * @param assignments
   *   For each `event_name` - `None` being the events that have none - the partitions its rows go
   *   to. A key that was not split has one entry; a key split into k pieces has k, and which one a
   *   row uses is decided by hashing its `event_id`. The same partition may appear twice, meaning
   *   two pieces of that key share a partition and merge back into one output file.
   *
   * Ordered by descending event count, which callers depend on: `SparkUtils.partitionIdColumn`
   * turns this into a CASE, and Spark evaluates a CASE as a short-circuiting chain in branch order,
   * so the keys that most rows have must come first. Ties break on name, so the same histogram
   * always yields the same plan.
   * @param fallbackPartition
   *   Where rows go when `event_name` matches no key. Unreachable, because the histogram is built
   *   from the same events as the rows; it exists only because a CASE needs an else branch.
   * @param partitionLoads
   *   The event count the bin-packer expects in each partition. Logged by
   *   `SparkUtils.describePlan`, and what `WriterPartitionerSpec` reads to assert the imbalance
   *   bounds - nothing on the window's path consults it.
   */
  case class Plan(
    numPartitions: Int,
    assignments: Vector[(Option[String], Vector[Int])],
    fallbackPartition: Int,
    partitionLoads: Vector[Long]
  )

  def plan(
    eventNameCounts: Map[Option[String], Int],
    writerParallelism: Int,
    config: Config.WriterPartitioning
  ): Plan = {
    val numPartitions = writerParallelism.max(1)
    val totalEvents   = eventNameCounts.values.map(_.toLong).sum
    val fairShare     = totalEvents.toDouble / numPartitions

    // Two candidate plans. The first cuts the fewest keys that could balance anything - only those
    // that cannot fit in a partition. The second cuts every key the floor allows.
    //
    // The second is needed because greedy packing is only as even as its largest item, and a key at
    // or below a fair share is not a candidate for the first pass - so `writerParallelism + 1`
    // similar keys packs two whole keys into one partition however the settings are tuned.
    //
    // The second wins only by the margin below, so a window that packs evenly whole is never cut up
    // for nothing. Comparing the two plans rather than testing the first against a threshold is
    // what lets that be a judgement about this window: no fixed threshold manages it, since one
    // loose enough to leave an even window alone also declines the cases the second plan exists
    // for. Both plans are arithmetic over at most `numPartitions * splitsPerFairShare` items, so
    // computing the one we discard is far cheaper than choosing between them badly.
    val whole = pack(eventNameCounts, numPartitions, choosePieceCounts(eventNameCounts, fairShare, totalEvents, numPartitions, config))
    val cut   = pack(eventNameCounts, numPartitions, choosePieceCounts(eventNameCounts, 0d, totalEvents, numPartitions, config))

    // By a margin, not by any improvement at all: the second plan pays an output file per extra
    // piece, and a hair's-breadth gain in the heaviest partition does not earn that. Relative
    // rather than absolute, because what it is buying - a shorter commit - is proportional.
    val chosen = if (cut.heaviest < whole.heaviest * WorthSplittingFor) cut else whole

    // Hottest first, for the reason given on Plan.assignments. Ordering[Option[String]] is a total
    // order, so a window holding both a null event_name and one named "" still sorts predictably.
    val ordered = chosen.assignments.toVector.sortBy { case (name, _) => (-eventNameCounts.getOrElse(name, 0), name) }

    Plan(numPartitions, ordered, lightest(chosen.loads), chosen.loads)
  }

  /**
   * One candidate packing: what each partition would hold, and where each key's pieces would go.
   */
  private case class Packing(loads: Vector[Long], assignments: Map[Option[String], Vector[Int]]) {
    def heaviest: Long = loads.max
  }

  /** Greedy largest-first bin packing of the pieces into `numPartitions` bins. */
  private def pack(
    eventNameCounts: Map[Option[String], Int],
    numPartitions: Int,
    pieceCounts: Map[Option[String], Int]
  ): Packing = {
    // One item per piece, weighted by the events it will carry. Sorted largest first, with name and
    // piece index breaking ties so that the same histogram always produces the same plan.
    val items = eventNameCounts.toList
      .flatMap { case (name, count) =>
        val pieces    = pieceCounts.getOrElse(name, 1)
        val perPiece  = count / pieces
        val remainder = count % pieces
        (0 until pieces).map(i => (name, i, (perPiece + (if (i < remainder) 1 else 0)).toLong))
      }
      .sortBy { case (name, piece, weight) => (-weight, name, piece) }

    val empty = Packing(Vector.fill(numPartitions)(0L), Map.empty)
    items.foldLeft(empty) { case (Packing(loads, assignments), (name, _, weight)) =>
      val bin = lightest(loads)
      Packing(loads.updated(bin, loads(bin) + weight), assignments.updated(name, assignments.getOrElse(name, Vector.empty) :+ bin))
    }
  }

  private def lightest(loads: Vector[Long]): Int =
    loads.indices.minBy(i => (loads(i), i))

  /**
   * Event names bigger than `threshold`, mapped to the number of pieces to cut them into.
   *
   * The first plan passes a fair share, so it considers only a name that cannot fit in one
   * partition. The second passes zero and lets the arithmetic decide, which needs no threshold of
   * its own: `wanted` is one for any name holding at most `totalEvents / slices` events, and
   * `permitted` is one for any name below twice `minEventsPerSplit`, so either way the filter below
   * drops it.
   *
   * Every degenerate window falls out of the three caps rather than needing a guard. An empty
   * histogram never reaches the division, there being no names to iterate. A window holding fewer
   * events than `slices` gives `wanted` more pieces than the name has events, which `.min(count)`
   * takes back - that cap, not `permitted`, is what holds when `minEventsPerSplit` is 1.
   *
   * `wanted` cannot overflow: `count` is at most `totalEvents`, so the quotient is bounded by
   * `slices`, and the numerator by `totalEvents * slices`.
   */
  private def choosePieceCounts(
    eventNameCounts: Map[Option[String], Int],
    threshold: Double,
    totalEvents: Long,
    numPartitions: Int,
    config: Config.WriterPartitioning
  ): Map[Option[String], Int] = {
    // Pieces of `totalEvents / slices` events, which is a fair share divided by `splitsPerFairShare`.
    val slices = numPartitions.toLong * config.splitsPerFairShare.max(1)
    eventNameCounts
      .collect {
        // How finely we would like to cut, against how finely the floor permits. The floor is
        // integer division, so every piece holds at least `minEventsPerSplit` events - which is the
        // point of applying it here rather than to the size aimed for, where rounding the piece
        // count up would round the piece size back down through it. There is no point having more
        // pieces than events, hence the last cap.
        case (name, count) if count > threshold =>
          val wanted    = ceilDiv(count.toLong * slices, totalEvents.max(1))
          val permitted = (count / config.minEventsPerSplit.max(1)).max(1).toLong
          name -> wanted.min(permitted).min(count.toLong).toInt
      }
      .filter { case (_, pieces) =>
        // A name the floor has kept whole comes out of the arithmetic above as a single piece.
        pieces > 1
      }
  }

  /**
   * `ceil(a / b)` for non-negative longs.
   *
   * Integer arithmetic rather than `math.ceil` over a `Double`, because the ratio here is routinely
   * an exact integer - a window of a single `event_name` asks for exactly `slices` pieces - and
   * floating-point division can land just above one. A single piece too many gives one partition an
   * extra and leaves the rest short.
   */
  private def ceilDiv(a: Long, b: Long): Long =
    (a + b - 1) / b
}
