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

import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.lakes._

import scala.concurrent.duration.DurationLong

class LakeWriterSpec extends Specification with CatsEffect {
  import LakeWriterSpec._

  def is = s2"""
  The lake writer should:
    become healthy after creating the table $e1
    retry creating table and send alerts when there is a setup exception $e2
    retry creating table if there is a transient exception, with limited number of attempts and no monitoring alerts $e3
    become healthy after recovering from an earlier setup error $e4
    become healthy after recovering from an earlier transient error $e5
    become healthy after committing to the lake $e6
    become unhealthy after failure to commit to the lake $e7
  """

  def e1 =
    control().flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      for {
        _ <- wrappedLakeWriter.createTable
        state <- c.state.get
      } yield state should beEqualTo(expected)
    }

  def e2 = {
    val mocks = Mocks(List.fill(100)(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.SentAlert(0L),
        Action.CreateTableAttempted,
        Action.SentAlert(30L),
        Action.CreateTableAttempted,
        Action.SentAlert(90L),
        Action.CreateTableAttempted,
        Action.SentAlert(210L)
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        _ => "this is a setup error"
      )

      val test = for {
        fiber <- wrappedLakeWriter.createTable.voidError.start
        _ <- IO.sleep(4.minutes)
        _ <- fiber.cancel
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e3 = {
    val mocks = Mocks(List.fill(100)(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter)
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      val test = for {
        _ <- wrappedLakeWriter.createTable.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e4 = {
    val mocks = Mocks(List(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.SentAlert(0L),
        Action.CreateTableAttempted,
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        _ => "this is a setup error"
      )

      val test = for {
        _ <- wrappedLakeWriter.createTable.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e5 = {
    val mocks = Mocks(List(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.BecameUnhealthy(RuntimeService.SparkWriter),
        Action.CreateTableAttempted,
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      val test = for {
        _ <- wrappedLakeWriter.createTable.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e6 =
    control().flatMap { c =>
      val expected = Vector(
        Action.CommitAttempted("testview"),
        Action.BecameHealthy(RuntimeService.SparkWriter)
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      for {
        _ <- wrappedLakeWriter.commit("testview")
        state <- c.state.get
      } yield state should beEqualTo(expected)
    }

  def e7 = {
    val mocks = Mocks(List(Response.Success, Response.ExceptionThrown(new RuntimeException("boom!"))))

    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CommitAttempted("testview1"),
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.CommitAttempted("testview2"),
        Action.BecameUnhealthy(RuntimeService.SparkWriter)
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(
        c.lakeWriter,
        c.appHealth,
        retriesConfig,
        dummyDestinationSetupErrorCheck
      )

      for {
        _ <- wrappedLakeWriter.commit("testview1")
        _ <- wrappedLakeWriter.commit("testview2").voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)
    }
  }

}

object LakeWriterSpec {
  sealed trait Action

  object Action {
    case object CreateTableAttempted extends Action
    case class CommitAttempted(viewName: String) extends Action
    case class SentAlert(timeSentSeconds: Long) extends Action
    case class BecameUnhealthy(service: RuntimeService) extends Action
    case class BecameHealthy(service: RuntimeService) extends Action
    case object BecameHealthyForSetup extends Action
  }

  sealed trait Response
  object Response {
    case object Success extends Response
    final case class ExceptionThrown(value: Throwable) extends Response
  }

  case class Mocks(lakeWriterResults: List[Response])

  case class Control(
    state: Ref[IO, Vector[Action]],
    lakeWriter: LakeWriter[IO],
    appHealth: AppHealth.Interface[IO, Alert, RuntimeService]
  )

  val retriesConfig = Config.Retries(
    Retrying.Config.ForSetup(30.seconds),
    Retrying.Config.ForTransient(1.second, 5)
  )

  def control(mocks: Mocks = Mocks(Nil)): IO[Control] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      tableManager <- testLakeWriter(state, mocks.lakeWriterResults)
    } yield Control(state, tableManager, testAppHealth(state))

  private def testAppHealth(state: Ref[IO, Vector[Action]]): AppHealth.Interface[IO, Alert, RuntimeService] =
    new AppHealth.Interface[IO, Alert, RuntimeService] {
      def beHealthyForSetup: IO[Unit] =
        state.update(_ :+ Action.BecameHealthyForSetup)
      def beUnhealthyForSetup(alert: Alert): IO[Unit] =
        for {
          now <- IO.realTime
          _ <- state.update(_ :+ Action.SentAlert(now.toSeconds))
        } yield ()
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        state.update(_ :+ Action.BecameHealthy(service))
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        state.update(_ :+ Action.BecameUnhealthy(service))
    }

  private val dummyDestinationSetupErrorCheck: PartialFunction[Throwable, String] = PartialFunction.empty

  private def testLakeWriter(state: Ref[IO, Vector[Action]], mocks: List[Response]): IO[LakeWriter[IO]] =
    for {
      mocksRef <- Ref[IO].of(mocks)
    } yield new LakeWriter[IO] {
      def createTable: IO[Unit] =
        for {
          response <- mocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.CreateTableAttempted)
          result <- response match {
                      case Response.Success =>
                        IO.unit
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { case t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result

      def initializeLocalDataFrame(viewName: String): IO[Unit] = IO.unit

      def localAppendRows(
        viewName: String,
        rows: NonEmptyList[Row],
        schema: StructType
      ): IO[Unit] = IO.unit

      def removeDataFrameFromDisk(viewName: String): IO[Unit] = IO.unit

      def commit(viewName: String): IO[Unit] =
        for {
          response <- mocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.CommitAttempted(viewName))
          result <- response match {
                      case Response.Success =>
                        IO.unit
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { case t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result
    }

}
