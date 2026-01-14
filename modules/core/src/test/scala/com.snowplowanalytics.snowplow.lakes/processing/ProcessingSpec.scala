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
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import io.circe.Json
import cats.effect.testkit.TestControl

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.lakes.{MockEnvironment, RuntimeService}
import com.snowplowanalytics.snowplow.lakes.MockEnvironment.Action

class ProcessingSpec extends Specification with CatsEffect {

  def is = s2"""
  The lake loader should:
    Ack received events at the end of a single window $e1
    Send badly formatted events to the bad sink $e2
    Write multiple windows of events in order $e3
    Write multiple batches in a single window when batch exceeds cutoff $e4
    Write good batches and bad events when a window contains both $e5
    Load events with a known schema $e6
    Send failed events for an unrecognized schema $e7
    Crash and exit for an unrecognized schema, if exitOnMissingIgluSchema is true $e8
    Crash and exit if events have schemas with clashing column names $e9
  """

  def e1 = {
    val io = for {
      inputs <- EventUtils.inputEvents(2, EventUtils.good())
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 4),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetE2ELatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetTableDataFilesTotal(123L),
        Action.SetTableSnaphotsRetained(456L),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e2 = {
    val io = for {
      tokened <- List.fill(3)(EventUtils.badlyFormatted).sequence
      control <- MockEnvironment.build(List(tokened))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e3 = {
    val io = for {
      inputs1 <- EventUtils.inputEvents(1, EventUtils.good())
      window1 <- inputs1.traverse(_.tokened)
      inputs2 <- EventUtils.inputEvents(3, EventUtils.good())
      window2 <- inputs2.traverse(_.tokened)
      inputs3 <- EventUtils.inputEvents(2, EventUtils.good())
      window3 <- inputs3.traverse(_.tokened)
      control <- MockEnvironment.build(List(window1, window2, window3))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,

        /* window 1 */
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 2),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(2),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetE2ELatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetTableDataFilesTotal(123L),
        Action.SetTableSnaphotsRetained(456L),
        Action.Checkpointed(window1.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000"),

        /* window 2 */
        Action.InitializedLocalDataFrame("v19700101000052"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000052", 6),
        Action.CommittedToTheLake("v19700101000052"),
        Action.AddedCommittedCountMetric(6),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.SetE2ELatencyMetric(2 * MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetTableDataFilesTotal(123L),
        Action.SetTableSnaphotsRetained(456L),
        Action.Checkpointed(window2.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000052"),

        /* window 3 */
        Action.InitializedLocalDataFrame("v19700101000134"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000134", 4),
        Action.CommittedToTheLake("v19700101000134"),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.SetE2ELatencyMetric(3 * MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetTableDataFilesTotal(123L),
        Action.SetTableSnaphotsRetained(456L),
        Action.Checkpointed(window3.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000134")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e4 = {
    val io = for {
      inputs <- EventUtils.inputEvents(3, EventUtils.good())
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      environment = control.environment.copy(inMemBatchBytes = 1L) // Drop the allowed max bytes
      _ <- Processing.stream(environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 2),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(6),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetE2ELatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetTableDataFilesTotal(123L),
        Action.SetTableSnaphotsRetained(456L),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e5 = {
    val io = for {
      bads1 <- List.fill(3)(EventUtils.badlyFormatted).sequence
      goods1 <- EventUtils.inputEvents(3, EventUtils.good()).flatMap(_.traverse(_.tokened))
      bads2 <- List.fill(1)(EventUtils.badlyFormatted).sequence
      goods2 <- EventUtils.inputEvents(1, EventUtils.good()).flatMap(_.traverse(_.tokened))
      control <- MockEnvironment.build(List(bads1 ::: goods1 ::: bads2 ::: goods2))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 8),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(8),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetE2ELatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetTableDataFilesTotal(123L),
        Action.SetTableSnaphotsRetained(456L),
        Action.Checkpointed((bads1 ::: goods1 ::: bads2 ::: goods2).map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e6 = {

    val ueGood700 = SnowplowEvent.UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val io = for {
      inputs <- EventUtils.inputEvents(1, EventUtils.good(ue = ueGood700))
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 2),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(2),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetE2ELatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetTableDataFilesTotal(123L),
        Action.SetTableSnaphotsRetained(456L),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e7 = {

    val ueDoesNotExist = SnowplowEvent.UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "doesnotexit", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val io = for {
      inputs <- EventUtils.inputEvents(1, EventUtils.good(ue = ueDoesNotExist))
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(1),
        Action.SentToBad(1),
        Action.AppendedRowsToDataFrame("v19700101000000", 1),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(1),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetE2ELatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.SetTableDataFilesTotal(123L),
        Action.SetTableSnaphotsRetained(456L),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e8 = {

    val ueDoesNotExist = SnowplowEvent.UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "doesnotexit", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val io = for {
      inputs <- EventUtils.inputEvents(1, EventUtils.good(ue = ueDoesNotExist))
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      environment = control.environment.copy(exitOnMissingIgluSchema = true)
      _ <- Processing.stream(environment).compile.drain.voidError
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.BecameUnhealthy(RuntimeService.Iglu),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e9 = {

    val contextsWithClashingSchemas = SnowplowEvent.Contexts(
      List(
        SelfDescribingData(
          SchemaKey("clashing.a", "b_c", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        ),
        SelfDescribingData(
          SchemaKey("clashing", "a_b_c", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val io = (for {
      inputs <- EventUtils.inputEvents(1, EventUtils.good(contexts = contextsWithClashingSchemas))
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      environment = control.environment
      _ <- Processing.stream(environment).compile.drain.void
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.BecameUnhealthy(RuntimeService.Iglu),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )).handleError { e =>
      e.getMessage must beEqualTo("schemas [clashing.a.b_c, clashing.a_b_c] have clashing column names")
    }

    TestControl.executeEmbed(io)
  }

}
