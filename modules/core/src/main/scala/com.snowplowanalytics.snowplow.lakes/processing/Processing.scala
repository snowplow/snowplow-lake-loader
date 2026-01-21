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

import cats.implicits._
import cats.data.NonEmptyList
import cats.{Applicative, Foldable, Functor}
import cats.effect.{Async, Deferred, Sync}
import cats.effect.kernel.{Ref, Unique}
import fs2.{Chunk, Pipe, Stream}
import io.circe.syntax._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.Instant
import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, ListOfList, TokenedEvents}
import com.snowplowanalytics.snowplow.lakes.{Environment, RuntimeService}
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
import com.snowplowanalytics.snowplow.loaders.transform.{
  BadRowsSerializer,
  NonAtomicFields,
  SchemaSubVersion,
  TabledEntity,
  Transform,
  TypedTabledEntity
}

object Processing {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] =
    Stream.eval(Deferred[F, Unit]).flatMap { deferredTableExists =>
      val runInBackground =
        // Create the table in a background stream, so it does not block subscribing to the stream.
        // Needed for Kinesis, where we want to subscribe to the stream as early as possible, so that other workers don't steal our shard leases
        Stream.eval(env.lakeWriter.createTable *> deferredTableExists.complete(()))

      implicit val lookup: RegistryLookup[F] = Http4sRegistryLookup(env.httpClient)
      val eventProcessingConfig              = EventProcessingConfig(env.windowing, env.metrics.setLatency)

      Stream.eval(WindowState.factory[F]).flatMap { windowStateFactory =>
        env.source
          .stream(eventProcessingConfig, eventProcessor(env, deferredTableExists.get, windowStateFactory))
          .concurrently(runInBackground)
      }

    }

  /** Model used between stages of the processing pipeline */

  private case class ParseResult(
    events: List[Event],
    bad: List[BadRow],
    originalBytes: Long,
    earliestCollectorTstamp: Option[Instant]
  )

  private case class Batched(
    events: ListOfList[Event],
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    originalBytes: Long,
    earliestCollectorTstamp: Option[Instant]
  )

  private def eventProcessor[F[_]: Async: RegistryLookup](
    env: Environment[F],
    deferredTableExists: F[Unit],
    windowStateFactory: WindowState.Factory[F]
  ): EventProcessor[F] = { in =>
    val resources = for {
      windowState <- Stream.eval(windowStateFactory.build)
      _ <- Stream.eval(deferredTableExists)
      stateRef <- Stream.eval(Ref[F].of(windowState))
      _ <- manageDataFrame(env, windowState.viewName)
    } yield stateRef

    val badProcessor = BadRowProcessor(env.appInfo.name, env.appInfo.version)

    resources.flatMap { stateRef =>
      in.through(processBatches(env, badProcessor, stateRef))
        .append(finalizeWindow(env, stateRef))
    }
  }

  /**
   * Manages the lifecycle of initializing and cleaning a Spark DataFrame scoped to the lifetime of
   * the window
   */
  private def manageDataFrame[F[_]](env: Environment[F], viewName: String): Stream[F, Unit] = {
    val init = env.lakeWriter.initializeLocalDataFrame(viewName)
    val drop = env.lakeWriter.removeDataFrameFromDisk(viewName)
    Stream.bracket(init)(_ => drop)
  }

  private def processBatches[F[_]: Async: RegistryLookup](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    ref: Ref[F, WindowState]
  ): Pipe[F, TokenedEvents, Nothing] =
    _.through(rememberTokens(ref))
      .through(incrementReceivedCount(env))
      .through(parseBytes(env, badProcessor))
      .through(handleParseFailures(env, badProcessor))
      .through(BatchUp.noTimeout(env.inMemBatchBytes))
      .through(transformBatch(env, badProcessor, ref))

  private def transformBatch[F[_]: RegistryLookup: Async](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    ref: Ref[F, WindowState]
  ): Pipe[F, Batched, Nothing] =
    _.parEvalMapUnordered(env.cpuParallelism) { case Batched(events, entities, _, earliestCollectorTstamp) =>
      for {
        _ <- Logger[F].debug(s"Processing batch of size ${events.size}")
        resolveTypesResult <- NonAtomicFields.resolveTypes[F](env.resolver, entities, env.schemasToSkip)
        nonAtomicFields <- possiblyExitOnClashingIgluSchemas(env, resolveTypesResult)
        _ <- possiblyExitOnMissingIgluSchema(env, nonAtomicFields)
        _ <- rememberColumnNames(ref, nonAtomicFields.fields)
        (bad, rows) <- transformToSpark[F](badProcessor, events, nonAtomicFields)
        _ <- sendFailedEvents(env, badProcessor, bad)
        windowState <- ref.updateAndGet { s =>
                         val updatedCollectorTstamp = chooseEarliestTstamp(earliestCollectorTstamp, s.earliestCollectorTstamp)
                         s.copy(numEvents = s.numEvents + rows.size, earliestCollectorTstamp = updatedCollectorTstamp)
                       }
        _ <- sinkTransformedBatch(env, windowState, rows, SparkSchema.forBatch(nonAtomicFields.fields, env.respectIgluNullability))
      } yield ()
    }.drain

  private def sinkTransformedBatch[F[_]: Sync](
    env: Environment[F],
    windowState: WindowState,
    rows: List[Row],
    schema: StructType
  ): F[Unit] =
    NonEmptyList.fromList(rows) match {
      case Some(nel) =>
        for {
          _ <- env.lakeWriter.localAppendRows(windowState.viewName, nel, schema)
          _ <- Logger[F].debug(s"Finished processing batch of size ${rows.size}")
        } yield ()
      case None =>
        Logger[F].debug(s"An in-memory batch yielded zero good events.  Nothing will be saved to local disk.")
    }

  private def rememberTokens[F[_]: Functor](ref: Ref[F, WindowState]): Pipe[F, TokenedEvents, Chunk[ByteBuffer]] =
    _.evalMap { case TokenedEvents(events, token) =>
      ref.update(state => state.copy(tokens = token :: state.tokens)).as(events)
    }

  private def incrementReceivedCount[F[_]](env: Environment[F]): Pipe[F, Chunk[ByteBuffer], Chunk[ByteBuffer]] =
    _.evalTap { events =>
      env.metrics.addReceived(events.size)
    }

  private def rememberColumnNames[F[_]](ref: Ref[F, WindowState], fields: Vector[TypedTabledEntity]): F[Unit] = {
    val colNames = fields.flatMap { typedTabledEntity =>
      typedTabledEntity.mergedField.name :: typedTabledEntity.recoveries.map(_._2.name)
    }.toSet
    ref.update(state => state.copy(nonAtomicColumnNames = state.nonAtomicColumnNames ++ colNames))
  }

  private def parseBytes[F[_]: Async](
    env: Environment[F],
    processor: BadRowProcessor
  ): Pipe[F, Chunk[ByteBuffer], ParseResult] =
    _.parEvalMapUnordered(env.cpuParallelism) { chunk =>
      for {
        numBytes <- Sync[F].delay(Foldable[Chunk].sumBytes(chunk))
        (badRows, events) <- Foldable[Chunk].traverseSeparateUnordered(chunk) { byteBuffer =>
                               Sync[F].delay {
                                 Event.parseBytes(byteBuffer).toEither.leftMap { failure =>
                                   val payload = BadRowRawPayload(StandardCharsets.UTF_8.decode(byteBuffer).toString)
                                   BadRow.LoaderParsingError(processor, failure, payload)
                                 }
                               }
                             }
        earliestCollectorTstamp = events.view.map(_.collector_tstamp).minOption
      } yield ParseResult(events, badRows, numBytes, earliestCollectorTstamp)
    }

  private implicit def batchable: BatchUp.Batchable[ParseResult, Batched] = new BatchUp.Batchable[ParseResult, Batched] {
    def combine(b: Batched, a: ParseResult): Batched = {
      val entities = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_))
      Batched(
        b.events.prepend(a.events),
        entities |+| b.entities,
        a.originalBytes + b.originalBytes,
        chooseEarliestTstamp(a.earliestCollectorTstamp, b.earliestCollectorTstamp)
      )
    }
    def single(a: ParseResult): Batched = {
      val entities = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_))
      Batched(ListOfList.of(List(a.events)), entities, a.originalBytes, a.earliestCollectorTstamp)
    }
    def weightOf(a: ParseResult): Long = a.originalBytes
  }

  // The pure computation is wrapped in a F to help the Cats Effect runtime to periodically cede to other fibers
  private def transformToSpark[F[_]: Sync](
    processor: BadRowProcessor,
    events: ListOfList[Event],
    entities: NonAtomicFields.Result
  ): F[(List[BadRow], List[Row])] =
    Foldable[ListOfList].traverseSeparateUnordered(events) { event =>
      Sync[F].delay {
        Transform
          .transformEvent[Any](processor, SparkCaster, event, entities)
          .map(SparkCaster.row(_))
      }
    }

  private def handleParseFailures[F[_]: Sync, A](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): Pipe[F, ParseResult, ParseResult] =
    _.evalTap { batch =>
      sendFailedEvents(env, badProcessor, batch.bad)
    }

  private def sendFailedEvents[F[_]: Sync, A](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    bad: List[BadRow]
  ): F[Unit] =
    if (bad.nonEmpty) {
      val serialized = bad.map(badRow => BadRowsSerializer.withMaxSize(badRow, badProcessor, env.badRowMaxSize))
      env.metrics.addBad(bad.size) *>
        env.badSink
          .sinkSimple(ListOfList.of(List(serialized)))
          .onError { case _ =>
            env.appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)
          }
    } else Applicative[F].unit

  private def finalizeWindow[F[_]: Sync](
    env: Environment[F],
    ref: Ref[F, WindowState]
  ): Stream[F, Unique.Token] =
    Stream.eval(ref.get).flatMap { state =>
      val commit = if (state.numEvents > 0) {
        for {
          _ <- Logger[F].info(s"Window ${state.viewName} ready to write and commit ${state.numEvents} events to the lake.")
          _ <- Logger[F].info(s"Non atomic columns: [${state.nonAtomicColumnNames.toSeq.sorted.mkString(",")}]")
          _ <- env.lakeWriter.commit(state.viewName)
          now <- Sync[F].realTime
          _ <- Logger[F].info(s"Window ${state.viewName} finished writing and committing ${state.numEvents} events to the lake.")
          _ <- env.metrics.addCommitted(state.numEvents)
          _ <- env.metrics.setProcessingLatency(now - state.startTime.toEpochMilli.millis)
          _ <- state.earliestCollectorTstamp match {
                 case Some(earliestCollectorTstamp) =>
                   env.metrics.setE2ELatency(now - earliestCollectorTstamp.toEpochMilli.millis)
                 case None =>
                   Sync[F].unit
               }
          tableDataFilesTotal <- env.lakeWriter.getTableDataFilesTotal
          _ <- tableDataFilesTotal.fold(Sync[F].unit)(env.metrics.setTableDataFilesTotal)
          tableSnapshotsRetained <- env.lakeWriter.getTableSnapshotsRetained
          _ <- tableSnapshotsRetained.fold(Sync[F].unit)(env.metrics.setTableSnapshotsRetained)
        } yield ()
      } else
        Logger[F].info(s"Window ${state.viewName} yielded zero good events.  Nothing will be written into the lake.")

      Stream.eval(commit) >> Stream.emits(state.tokens.reverse)
    }

  private def possiblyExitOnMissingIgluSchema[F[_]: Sync](env: Environment[F], nonAtomicFields: NonAtomicFields.Result): F[Unit] =
    if (env.exitOnMissingIgluSchema && nonAtomicFields.igluFailures.nonEmpty) {
      val base =
        "Exiting because failed to resolve Iglu schemas.  Either check the configuration of the Iglu repos, or set the `skipSchemas` config option, or set `exitOnMissingIgluSchema` to false.\n"
      val msg = nonAtomicFields.igluFailures.map(_.failure.asJson.noSpaces).mkString(base, "\n", "")
      Logger[F].error(base) *> env.appHealth.beUnhealthyForRuntimeService(RuntimeService.Iglu) *> Sync[F].raiseError(
        new RuntimeException(msg)
      )
    } else Applicative[F].unit

  private def possiblyExitOnClashingIgluSchemas[F[_]: Sync](
    env: Environment[F],
    resolveTypesResult: Either[NonAtomicFields.ResolveTypesException, NonAtomicFields.Result]
  ): F[NonAtomicFields.Result] =
    resolveTypesResult match {
      case Left(e) =>
        Logger[F].error(e.getMessage) *> env.appHealth.beUnhealthyForRuntimeService(RuntimeService.Iglu) *> Sync[F].raiseError(e)
      case Right(r) => Applicative[F].pure(r)
    }

  private def chooseEarliestTstamp(o1: Option[Instant], o2: Option[Instant]): Option[Instant] =
    (o1, o2)
      .mapN { case (t1, t2) =>
        if (t1.isBefore(t2)) t1 else t2
      }
      .orElse(o1)
      .orElse(o2)

}
