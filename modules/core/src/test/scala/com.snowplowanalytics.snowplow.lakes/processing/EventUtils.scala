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
import cats.effect.kernel.Unique
import cats.syntax.traverse._

import com.github.luben.zstd.ZstdOutputStream
import fs2.{Chunk, Stream}
import com.snowplowanalytics.snowplow.streams.TokenedEvents
import com.snowplowanalytics.snowplow.streams.compression.{Compressor, GzipCompressor, ZstdCompressor}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}

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

  def tokenedInputs(count: Long, source: IO[TokenedEvents]): IO[List[TokenedEvents]] =
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

  private val zstdFactory = ZstdCompressor.factory(3)
  private val gzipFactory = GzipCompressor.factory(6)

  /** Two events compressed into a single zstd-compressed Snowplow record */
  def goodZstdCompressed: IO[TokenedEvents] =
    mkBytes(2).map { case (ack, bytes) =>
      TokenedEvents(Chunk(compress(zstdFactory, bytes)), ack)
    }

  /** Two events compressed into a single gzip-compressed Snowplow record */
  def goodGzipCompressed: IO[TokenedEvents] =
    mkBytes(2).map { case (ack, bytes) =>
      TokenedEvents(Chunk(compress(gzipFactory, bytes)), ack)
    }

  /**
   * Three events — one plain, one zstd-compressed, one gzip-compressed — in the same TokenedEvents
   */
  def goodMixed: IO[TokenedEvents] =
    mkBytes(3).map { case (ack, bytes) =>
      val plain          = ByteBuffer.wrap(bytes(0))
      val zstdCompressed = compress(zstdFactory, List(bytes(1)))
      val gzipCompressed = compress(gzipFactory, List(bytes(2)))
      TokenedEvents(Chunk(plain, zstdCompressed, gzipCompressed), ack)
    }

  /**
   * A zstd buffer whose Snowplow framing claims a 10-byte record but only contains 3 — the
   * decompressor will reject the whole payload as corrupt.
   */
  def corruptZstdCompressed: IO[TokenedEvents] =
    IO.unique.map { token =>
      val baos = new java.io.ByteArrayOutputStream()
      val zstd = new ZstdOutputStream(baos)
      zstd.write(1) // compression format version
      zstd.write(1) // payload format version
      val sizeBytes = ByteBuffer.allocate(4)
      sizeBytes.order(ByteOrder.BIG_ENDIAN)
      sizeBytes.putInt(10)
      zstd.write(sizeBytes.array())
      zstd.write(Array[Byte](1, 2, 3))
      zstd.close()
      TokenedEvents(Chunk(ByteBuffer.wrap(baos.toByteArray)), token)
    }

  /**
   * A zstd record containing one parseable event and one record whose decompressed size exceeds
   * `maxBytesSinglePayload` — the decompressor emits a SizeViolation alongside the good record.
   */
  def goodWithOversizedRecord(oversizedBytes: Int): IO[TokenedEvents] =
    for {
      ack <- IO.unique
      id <- IO.randomUUID
      now <- IO.realTimeInstant
    } yield {
      val good      = Event.minimal(id, now, "0.0.0", "0.0.0").toTsv.getBytes(StandardCharsets.UTF_8)
      val oversized = Array.fill[Byte](oversizedBytes)('a'.toByte)
      TokenedEvents(Chunk(compress(zstdFactory, List(good, oversized))), ack)
    }

  private def mkBytes(n: Int): IO[(Unique.Token, List[Array[Byte]])] =
    for {
      ack <- IO.unique
      ids <- List.fill(n)(IO.randomUUID).sequence
      now <- IO.realTimeInstant
    } yield {
      val bytes = ids.map(id => Event.minimal(id, now, "0.0.0", "0.0.0").toTsv.getBytes(StandardCharsets.UTF_8))
      (ack, bytes)
    }

  private def compress(factory: Compressor.Factory, tsvBytes: List[Array[Byte]]): ByteBuffer = {
    val compressor = factory.buildAndInitialize(1000000, 1)
    tsvBytes.foreach { bytes =>
      val _ = compressor.addRecord(bytes, 0, bytes.length)
    }
    compressor.result
  }

}
