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

import cats.effect.IO

import fs2.{Chunk, Stream}
import com.snowplowanalytics.snowplow.streams.TokenedEvents
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

object EventUtils {

  case class TestBatch(value: List[Event]) {
    def tokened: IO[TokenedEvents] = {
      val serialized = Chunk.from(value).map { e =>
        StandardCharsets.UTF_8.encode(e.toTsv)
      }
      IO.unique.map { ack =>
        TokenedEvents(serialized, ack)
      }
    }
  }

  def inputEvents(count: Long, source: IO[TestBatch]): IO[List[TestBatch]] =
    Stream
      .eval(source)
      .repeat
      .take(count)
      .compile
      .toList

  def good(ue: UnstructEvent = UnstructEvent(None), contexts: Contexts = Contexts(List.empty)): IO[TestBatch] =
    for {
      eventId1 <- IO.randomUUID
      eventId2 <- IO.randomUUID
      collectorTstamp <- IO.realTimeInstant
    } yield {
      val event1 = Event
        .minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0")
        .copy(tr_total = Some(1.23))
        .copy(unstruct_event = ue)
        .copy(contexts = contexts)
      val event2 = Event
        .minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
      TestBatch(List(event1, event2))
    }

  def badlyFormatted: IO[TokenedEvents] =
    IO.unique.map { token =>
      val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token)
    }

}
