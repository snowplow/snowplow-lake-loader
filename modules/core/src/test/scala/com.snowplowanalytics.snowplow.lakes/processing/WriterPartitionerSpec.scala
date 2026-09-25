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

import org.specs2.Specification

import com.snowplowanalytics.snowplow.lakes.Config

// WriterPartitioner is private[processing], so this test must live in the same package.
class WriterPartitionerSpec extends Specification {

  def is = s2"""
  WriterPartitioner.plan should:
    Use exactly writerParallelism partitions, never more, so a task slot stays free for the handover $e1
    Give every event name a partition $e2
    Not split any event name when none of them exceeds a fair share $e3
    Balance the partitions when no event name exceeds a fair share $e4
    Split an event name that exceeds a fair share, into pieces of splitsPerFairShare $e5
    Balance the partitions when one event name dominates the window $e6
    Keep a small event name whole when a big loader receives a slow trickle of events $e7
    Never cut a split smaller than minEventsPerSplit $e8
    Never split an event name into more pieces than it has events $e9
    Survive an empty histogram, which means a window that saw no events at all $e10
    Produce the same plan for the same histogram $e11
    Order the assignments hottest first, so the generated CASE short-circuits for most rows $e12
    Balance a window whose events all have a null event_name exactly as it would any one key $e13
    Split keys that each fit a partition, when leaving them whole would not pack evenly $e14
    Leave keys whole when they do pack evenly, so an even window is not fragmented for nothing $e15
    Never cut a piece holding fewer events than minEventsPerSplit $e16
    Never split an event name that has fewer than twice minEventsPerSplit events $e17
    Cut a window of one event name into exactly the pieces the arithmetic asks for $e18
    Keep the heaviest partition inside the imbalance bound Config.WriterPartitioning claims $e19
    Balance to within one piece of even once a window is big enough for the floor to be inactive $e20
    Never cut a window into more pieces than the settings ask for $e21
    Leave a window whole when splitting it would buy less balance than it costs in files $e22
  """

  private def plan(
    counts: Map[Option[String], Int],
    writerParallelism: Int  = 5,
    splitsPerFairShare: Int = 4,
    minEventsPerSplit: Int  = 1
  ) = WriterPartitioner.plan(counts, writerParallelism, Config.WriterPartitioning(splitsPerFairShare, minEventsPerSplit))

  /** The heaviest partition as a multiple of a perfectly even share. 1.0 is optimal. */
  private def imbalance(p: WriterPartitioner.Plan): Double = {
    val total = p.partitionLoads.sum
    p.partitionLoads.max.toDouble / (total.toDouble / p.numPartitions)
  }

  private val skewed =
    Map(name("hot") -> 600000, name("warm") -> 200000, name("mid") -> 100000) ++ (1 to 20).map(i => name(f"cold_$i%02d") -> 5000).toMap

  /** The histogram is keyed by the column's value, so a present event_name is a Some. */
  private def name(s: String): Option[String] = Some(s)

  // The invariant the whole design rests on. writerParallelism is availableProcessors - 1, so a
  // commit of that many tasks leaves one Spark task slot free, and the per-batch localCheckpoint
  // handover never has to wait for a commit task to finish. Spark does not preempt, so more
  // partitions than this means every slot is busy and the handover blocks inside the append mutex.
  def e1 = {
    val plans = List(
      plan(skewed, writerParallelism                                          = 6),
      plan(skewed, writerParallelism                                          = 31),
      plan(Map(name("a") -> 10), writerParallelism                            = 15),
      plan(Map.empty[Option[String], Int], writerParallelism                  = 7),
      plan((1 to 500).map(i => name(s"n$i") -> 1000).toMap, writerParallelism = 6)
    )
    (plans.map(_.numPartitions) must_== List(6, 31, 15, 7, 6)) and
      (plans.forall(p => p.assignments.flatMap(_._2).forall(i => i >= 0 && i < p.numPartitions)) must beTrue) and
      (plans.forall(p => p.fallbackPartition >= 0 && p.fallbackPartition < p.numPartitions) must beTrue)
  }

  def e2 = {
    val p = plan(skewed, writerParallelism = 6)
    p.assignments.map(_._1).toSet must_== skewed.keySet
  }

  // Five names of 200 each over 5 partitions: none is bigger than a fair share, so none is cut.
  def e3 = {
    val counts = List("a", "b", "c", "d", "e").map(name(_) -> 200).toMap
    plan(counts).assignments.map(_._2.length).distinct must_== Vector(1)
  }

  // ...and greedy largest-first packing puts exactly one of them in each partition.
  def e4 = {
    val counts = List("a", "b", "c", "d", "e").map(name(_) -> 200).toMap
    plan(counts).partitionLoads must_== Vector(200L, 200L, 200L, 200L, 200L)
  }

  // total 1000 over 5 partitions => a fair share is 200, and at splitsPerFairShare=4 a piece is 50.
  // "a" and "b" are over a fair share and are cut into 50-event pieces; "c" fits in one partition.
  def e5 = {
    val counts = Map(name("a") -> 600, name("b") -> 300, name("c") -> 100)
    val p      = plan(counts)
    p.assignments.map { case (name, partitions) => name -> partitions.length }.toMap must_== Map(
      name("a") -> 12,
      name("b") -> 6,
      name("c") -> 1
    )
  }

  // The point of splitting. Without it "hot" alone would be 600000 events in one partition against
  // a fair share of 155000, so the commit would take four times longer than it needs to.
  def e6 = imbalance(plan(skewed, writerParallelism = 6)) must beLessThan(1.05)

  // The case minEventsPerSplit exists for: a vertically large loader receiving a trickle. A fair
  // share is 3 events here, so purely relative arithmetic would cut "a" into 60 one-event pieces
  // and write a parquet file for each. The floor keeps both names whole.
  def e7 = {
    val counts = Map(name("a") -> 60, name("b") -> 40)
    plan(counts, writerParallelism = 31, minEventsPerSplit = 10000).assignments.map(_._2.length).distinct must_== Vector(1)
  }

  // A fair share is 20000 here and splitsPerFairShare would ask for 5000-event pieces, but the
  // floor overrides it, so both names are cut into 10000-event pieces instead.
  def e8 = {
    val counts = Map(name("a") -> 100000, name("b") -> 100000)
    val p      = plan(counts, writerParallelism = 10, minEventsPerSplit = 10000)
    p.assignments.map { case (name, partitions) => name -> partitions.length }.toMap must_== Map(name("a") -> 10, name("b") -> 10)
  }

  // total 8 over 6 partitions => a fair share is 1.33 and a piece is 0.33, so the arithmetic asks
  // for 21 pieces of an event name that only has 7 events.
  def e9 = {
    val counts = Map(name("a") -> 7, name("b") -> 1)
    plan(counts, writerParallelism = 6).assignments.toMap.apply(name("a")).length must_== 7
  }

  // The histogram counts every event, a null event_name under the None key, so it is empty only
  // for a window that saw no events at all - which still must not divide by zero or produce an
  // out-of-range partition.
  def e10 = {
    val p = plan(Map.empty[Option[String], Int], writerParallelism = 6)
    (p.assignments must beEmpty) and (p.numPartitions must_== 6) and (p.fallbackPartition must_== 0)
  }

  def e11 = plan(skewed, writerParallelism = 6) must_== plan(skewed, writerParallelism = 6)

  // SparkUtils.partitionIdColumn emits the branches of its CASE in exactly this order, and a CASE
  // short-circuits, so hottest first is what keeps most rows to a comparison or two. See
  // Plan.assignments.
  def e12 = {
    val p     = plan(skewed, writerParallelism = 6)
    val names = p.assignments.map(_._1)
    (names.take(3) must_== Vector(name("hot"), name("warm"), name("mid"))) and
      (names.drop(3) must_== (1 to 20).map(i => name(f"cold_$i%02d")).toVector)
  }

  // A null event_name is packed by the same arithmetic as any other key, so it needs no special
  // case. Asserted against the same window under a real name, so the two cannot drift apart.
  def e13 = {
    val nulls = plan(Map(None -> 1000000), writerParallelism = 15)
    val named = plan(Map(name("foo") -> 1000000), writerParallelism = 15)
    (nulls.partitionLoads must_== named.partitionLoads) and
      (imbalance(nulls) must beLessThan(1.01)) and
      (nulls.assignments.map(_._2.length) must_== named.assignments.map(_._2.length)) and
      (nulls.assignments.map(_._1) must_== Vector(None))
  }

  // Greedy packing is only as even as its largest item, and nothing at or below a fair share is a
  // split candidate on the first pass - so writerParallelism + 1 similar keys would put two whole
  // keys in one partition and leave the commit's critical path half as long again as it needs to
  // be. No setting fixes that, because the keys never become split candidates; the second pass is
  // what does. This is the shape e4 misses by using exactly writerParallelism keys, which is the
  // one count that packs perfectly whole.
  def e14 = {
    val counts = (1 to 7).map(i => name(f"k$i") -> 10000000).toMap
    val p      = plan(counts, writerParallelism = 6)
    (imbalance(p) must beLessThan(1.10)) and
      (p.assignments.map(_._2.length).distinct must_== Vector(4))
  }

  // The other side of that conditional. Splitting costs an output file per extra piece, so it must
  // not happen to a window the first pass already packs within the bound.
  def e15 = {
    val counts = (1 to 6).map(i => name(f"k$i") -> 10000000).toMap
    val p      = plan(counts, writerParallelism = 6)
    (imbalance(p) must beCloseTo(1.0, 0.01)) and
      (p.assignments.map(_._2.length).distinct must_== Vector(1))
  }

  /**
   * A deterministic spread of windows: core counts either side of a typical deployment, key counts
   * either side of the partition count, and volumes from a trickle to a busy window. Fixed seed, so
   * a failure is reproducible.
   */
  private val sweep: List[(Map[Option[String], Int], Int)] = {
    val rnd = new scala.util.Random(99)
    List
      .fill(3000) {
        val wp     = 2 + rnd.nextInt(7)
        val scale  = List(1000, 5000, 20000, 60000, 200000, 2000000)(rnd.nextInt(6))
        val counts = (1 to (1 + rnd.nextInt(wp + 4))).map(i => name(f"k$i%02d") -> (1 + rnd.nextInt(scale))).toMap
        (counts, wp)
      }
      .filter { case (counts, wp) => counts.values.map(_.toLong).sum >= wp }
  }

  private val sweepFloor  = 5000
  private val sweepSplits = 4

  private lazy val sweepPlans: List[(Map[Option[String], Int], Int, WriterPartitioner.Plan, Double)] =
    sweep.map { case (counts, wp) =>
      val p = plan(counts, writerParallelism = wp, splitsPerFairShare = sweepSplits, minEventsPerSplit = sweepFloor)
      (counts, wp, p, counts.values.map(_.toLong).sum.toDouble / wp)
    }

  // The floor caps the piece count rather than the size aimed for, which is what makes it a
  // guarantee: dividing by a floored target and rounding the count up would round the size back
  // down through it. Asserted over the sweep rather than one case, because the failure is a
  // rounding boundary and any single example only hits one of them.
  def e16 = {
    val offenders = sweep.flatMap { case (counts, wp) =>
      plan(counts, writerParallelism = wp, minEventsPerSplit = sweepFloor).assignments.collect {
        case (n, partitions) if partitions.length > 1 && counts(n).toDouble / partitions.length < sweepFloor =>
          (n, counts(n), partitions.length)
      }
    }
    offenders must beEmpty
  }

  // The same guarantee read from the other end, and the cost that comes with it: a key below twice
  // the floor cannot be cut at all, so a window of a few such keys spreads unevenly and no setting
  // changes that. Deliberate - such a window commits well inside its window however it is spread.
  def e17 = {
    val offenders = sweep.flatMap { case (counts, wp) =>
      plan(counts, writerParallelism = wp, minEventsPerSplit = sweepFloor).assignments.collect {
        case (n, partitions) if partitions.length > 1 && counts(n) < 2 * sweepFloor => (n, counts(n))
      }
    }
    offenders must beEmpty
  }

  // A window of a single event name asks for exactly writerParallelism * splitsPerFairShare pieces,
  // so the ratio is an exact integer and this asserts the count outright rather than a bound. One
  // piece too many gives a partition five where the rest have four, a quarter added to the critical
  // path of a window that should pack perfectly - and that sits inside every imbalance bound here,
  // so none of the examples below would notice it.
  def e18 = {
    val p = plan(Map(name("a") -> 1182637), writerParallelism = 15)
    (p.assignments.map(_._2.length) must_== Vector(60)) and
      (p.partitionLoads.distinct.length must beLessThan(3)) and
      (imbalance(p) must beCloseTo(1.0, 0.001))
  }

  // Greedy packing leaves the heaviest partition within one item of a fair share, and no item
  // exceeds the larger of the size aimed for and twice the floor - so that is the imbalance
  // `Config.WriterPartitioning` promises. Loose on a small window, where twice the floor dwarfs a
  // fair share, and deliberately so: such a window commits well inside its window however it is
  // spread. e20 is the same bound in the regime where it decides anything.
  def e19 = {
    val offenders = sweepPlans.filter { case (_, _, p, fairShare) =>
      val bound = 1 + math.max(fairShare / sweepSplits, 2.0 * sweepFloor) / fairShare
      p.partitionLoads.max / fairShare > bound + 1e-9
    }
    offenders.map { case (counts, wp, p, fair) => (counts, wp, p.partitionLoads.max / fair) } must beEmpty
  }

  // The bound that decides how long a commit takes. Once a window is big enough that the floor is
  // inactive - a fair share over `splitsPerFairShare` above twice it - every item in the split plan
  // is at most that fraction of a fair share, so its heaviest partition is within the same fraction
  // of even. This is the property the whole design is bought for.
  //
  // Asserted at `1 + 1 / splitsPerFairShare`, which is what this sweep reaches, rather than at what
  // is strictly provable: `WorthSplittingFor` can retain a whole-key plan up to that over the
  // margin, so the guaranteed ceiling is a shade higher. Generated windows have not produced one
  // between the two, and the seed is fixed, so this is a regression pin rather than a proof.
  //
  // It is a bound, not a measure of how good the packing is, so it does not subsume the examples
  // above: an extra piece on a window that should pack perfectly costs a fifth of the commit and
  // still lands inside it, which is why e18 pins its piece count outright. What this catches is a
  // change that stops balancing at all - losing the split plan fails it, and e19.
  def e20 = {
    val inRegime = sweepPlans.filter { case (_, _, _, fairShare) => fairShare / sweepSplits >= 2.0 * sweepFloor }
    val offenders = inRegime.filter { case (_, _, p, fairShare) =>
      p.partitionLoads.max / fairShare > 1 + 1.0 / sweepSplits + 1e-9
    }
    List[org.specs2.matcher.MatchResult[Any]](
      // Without this the example would pass on a sweep that never reached the regime at all.
      inRegime.size must beGreaterThan(100),
      offenders.map { case (counts, wp, p, fair) => (counts, wp, p.partitionLoads.max / fair) } must beEmpty
    ).reduce(_ and _)
  }

  // Fragmentation has a ceiling, and it is the settings rather than the data that set it: a key
  // asks for at most `count * numPartitions * splitsPerFairShare / totalEvents` pieces rounded up,
  // so the window comes to at most `numPartitions * splitsPerFairShare` pieces plus one per key.
  // Without this, a change cutting more finely to chase balance would multiply the output files
  // with nothing to notice - the cost side of every trade in this file.
  def e21 = {
    val offenders = sweepPlans.filter { case (counts, wp, p, _) =>
      p.assignments.map(_._2.length.toLong).sum > wp.toLong * sweepSplits + counts.size
    }
    offenders.map { case (counts, wp, p, _) => (counts.size, wp, p.assignments.map(_._2.length).sum) } must beEmpty
  }

  // `WorthSplittingFor` is the whole of the fragmentation side of the trade, and nothing else here
  // reaches it: e15's window gives the two plans an identical heaviest partition, so its tie-break
  // decides that one, not the margin. This window is the case the margin exists for - cutting k1
  // and k2 as well buys 0.64% off the heaviest partition and costs five extra pieces, so the plan
  // that leaves them whole is the one to keep.
  def e22 = {
    val counts = Map(name("k0") -> 100274, name("k1") -> 31697, name("k2") -> 50609)
    val p      = plan(counts, writerParallelism = 3, splitsPerFairShare = 4, minEventsPerSplit = 5000)
    p.assignments.map { case (n, partitions) => n -> partitions.length }.toMap must_== Map(
      name("k0") -> 7,
      name("k1") -> 1,
      name("k2") -> 1
    )
  }
}
