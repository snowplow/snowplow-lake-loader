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

import cats.effect.kernel.{Ref, Unique}
import cats.effect.Sync
import cats.implicits._

import java.time.Instant

/**
 * Local in-memory state which is accumulated as a window gets processed
 *
 * The data cached in this state is needed when finalizing the window, i.e. flushing events from
 * disk to storage
 *
 * @param tokens
 *   Tokens given to us by the sources library. Emitting these tokens at the end of the window will
 *   trigger checkpointing/acking of the source events.
 * @param startTime
 *   The time this window was initially opened
 * @param nonAtomicColumnNames
 *   Names of the columns which will be written out by the loader
 * @param numEvents
 *   The number of events in this window
 * @param earliestCollectorTstamp
 *   The earliest collector_tstamp of all events seen in the window
 * @param id
 *   An id for this window, which is unique within the internals of a running loader. It increments
 *   from one.
 */
private[processing] case class WindowState(
  tokens: List[Unique.Token],
  startTime: Instant,
  nonAtomicColumnNames: Set[String],
  numEvents: Int,
  earliestCollectorTstamp: Option[Instant],
  id: Int
) {

  /** The name by which the current DataFrame is known to the Spark catalog */
  val viewName: String =
    f"v$id%010d"
}

private[processing] object WindowState {

  class Factory[F[_]: Sync] private[WindowState] (counter: Ref[F, Int]) {
    def build: F[WindowState] =
      for {
        now <- Sync[F].realTimeInstant
        i <- counter.updateAndGet(_ + 1)
      } yield WindowState(Nil, now, Set.empty, 0, None, i)
  }

  def factory[F[_]: Sync]: F[Factory[F]] =
    Ref[F].of(0).map(new Factory(_))
}
