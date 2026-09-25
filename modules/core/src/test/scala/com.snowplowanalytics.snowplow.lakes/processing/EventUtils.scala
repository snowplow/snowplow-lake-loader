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
import com.snowplowanalytics.snowplow.streams.compression.CompressorFactory
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

  /**
   * A batch whose events all carry the given `event_name`.
   *
   * `Event.minimal` leaves `event_name` unset, so every other fixture here yields a histogram of a
   * single `None` key. Use this where telling one key from another is the point.
   */
  def named(eventName: String): IO[TestBatch] =
    for {
      eventId1 <- IO.randomUUID
      eventId2 <- IO.randomUUID
      collectorTstamp <- IO.realTimeInstant
    } yield TestBatch(
      List(eventId1, eventId2).map { eventId =>
        Event
          .minimal(eventId, collectorTstamp, "0.0.0", "0.0.0")
          .copy(event_name = Some(eventName))
      }
    )

  def badlyFormatted: IO[TokenedEvents] =
    IO.unique.map { token =>
      val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token)
    }

  private val zstdFactory = CompressorFactory.zstd(3)
  private val gzipFactory = CompressorFactory.gzip(6)

  /** Two events compressed into a single zstd-compressed Snowplow record */
  def goodZstdCompressed: IO[TokenedEvents] =
    mkBytes(2).flatMap { case (ack, bytes) =>
      compress(zstdFactory, bytes).map(compressed => TokenedEvents(Chunk(compressed), ack))
    }

  /** Two events compressed into a single gzip-compressed Snowplow record */
  def goodGzipCompressed: IO[TokenedEvents] =
    mkBytes(2).flatMap { case (ack, bytes) =>
      compress(gzipFactory, bytes).map(compressed => TokenedEvents(Chunk(compressed), ack))
    }

  /**
   * Three events — one plain, one zstd-compressed, one gzip-compressed — in the same TokenedEvents
   */
  def goodMixed: IO[TokenedEvents] =
    mkBytes(3).flatMap { case (ack, bytes) =>
      for {
        zstdCompressed <- compress(zstdFactory, List(bytes(1)))
        gzipCompressed <- compress(gzipFactory, List(bytes(2)))
      } yield TokenedEvents(Chunk(ByteBuffer.wrap(bytes(0)), zstdCompressed, gzipCompressed), ack)
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
      good      = Event.minimal(id, now, "0.0.0", "0.0.0").toTsv.getBytes(StandardCharsets.UTF_8)
      oversized = Array.fill[Byte](oversizedBytes)('a'.toByte)
      compressed <- compress(zstdFactory, List(good, oversized))
    } yield TokenedEvents(Chunk(compressed), ack)

  private def mkBytes(n: Int): IO[(Unique.Token, List[Array[Byte]])] =
    for {
      ack <- IO.unique
      ids <- List.fill(n)(IO.randomUUID).sequence
      now <- IO.realTimeInstant
    } yield {
      val bytes = ids.map(id => Event.minimal(id, now, "0.0.0", "0.0.0").toTsv.getBytes(StandardCharsets.UTF_8))
      (ack, bytes)
    }

  private def compress(factory: CompressorFactory, tsvBytes: List[Array[Byte]]): IO[ByteBuffer] =
    factory.resource[IO].use { compressor =>
      IO {
        compressor.reset(payloadVersion = 1, targetSize = 1000000)
        tsvBytes.foreach { bytes =>
          val _ = compressor.addRecord(bytes, 0, bytes.length)
        }
        compressor.result
      }
    }

}
