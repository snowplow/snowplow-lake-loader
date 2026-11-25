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

import cats.effect.{Deferred, IO, Ref}
import cats.effect.testkit.TestControl
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.lakes._

import scala.concurrent.duration.DurationLong

/**
 * Every example runs under `TestControl.executeEmbed`, including those whose mocks never fail.
 *
 * `Retrying`'s transient policy is `RetryPolicies.fullJitter`, whose delay is random and doubles
 * per attempt, so an example reaching a backoff sleeps for real and can overrun `CatsEffect`'s
 * `Timeout`. `cats-effect-testing` reports a timeout as a skip rather than a failure, so such an
 * example asserts nothing and still leaves the suite green.
 *
 * An example whose mocks all succeed never reaches a backoff, so it passes without `executeEmbed`
 * until one of its mocks starts failing - hence on all of them rather than only the failing ones.
 */
class RemoteTableSpec extends Specification with CatsEffect {
  import RemoteTableSpec._

  def is = s2"""
  The remote table should:
    become healthy after creating the table $e1
    retry creating table and send alerts when there is a setup exception $e2
    retry creating table if there is a transient exception, with limited number of attempts and no monitoring alerts $e3
    become healthy after recovering from an earlier setup error $e4
    become healthy after recovering from an earlier transient error $e5
    become healthy after committing to the lake $e6
    become unhealthy after failure to commit to the lake $e7
    tell the first attempt at a commit that it is not a retry, and every later attempt that it is $e8
    not start a second window's commit while the first window's commit is retrying $e9
    report a table metric that fails to read as absent rather than failing $e10
  """

  def e1 =
    control().flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted,
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      )

      val test = for {
        remoteTable <- c.build(dummyDestinationSetupErrorCheck)
        _ <- remoteTable.create
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
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

      val test = for {
        remoteTable <- c.build(_ => "this is a setup error")
        fiber <- remoteTable.create.voidError.start
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

      val test = for {
        remoteTable <- c.build(dummyDestinationSetupErrorCheck)
        _ <- remoteTable.create.voidError
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

      val test = for {
        remoteTable <- c.build(_ => "this is a setup error")
        _ <- remoteTable.create.voidError
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

      val test = for {
        remoteTable <- c.build(dummyDestinationSetupErrorCheck)
        _ <- remoteTable.create.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e6 =
    control().flatMap { c =>
      val expected = Vector(
        Action.CommitAttempted("testview", isRetry = false),
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      )

      val test = for {
        remoteTable <- c.build(dummyDestinationSetupErrorCheck)
        _ <- remoteTable.commit("testview")
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }

  def e7 = {
    val numAttempts = retriesConfig.transientErrors.attempts
    val mocks = Mocks(
      List(Response.Success) ++ List.fill(numAttempts)(Response.ExceptionThrown(new RuntimeException("boom!")))
    )

    control(mocks).flatMap { c =>
      val commitAttempts = (1 to numAttempts).flatMap { attempt =>
        Vector(
          Action.CommitAttempted("testview2", isRetry = attempt > 1),
          Action.BecameUnhealthy(RuntimeService.SparkWriter)
        )
      }

      val expected = Vector(
        Action.CommitAttempted("testview1", isRetry = false),
        Action.BecameHealthy(RuntimeService.SparkWriter),
        Action.BecameHealthyForSetup
      ) ++ commitAttempts

      val test = for {
        remoteTable <- c.build(dummyDestinationSetupErrorCheck)
        _ <- remoteTable.commit("testview1")
        _ <- remoteTable.commit("testview2").voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  // The retry is what decides whether to look for the window in the table before writing it. The
  // first attempt must not look, which is what keeps the healthy path free of extra catalog calls,
  // and every later one must, because any failed attempt might have landed.
  def e8 = {
    val mocks = Mocks(List.fill(2)(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val test = for {
        remoteTable <- c.build(dummyDestinationSetupErrorCheck)
        _ <- remoteTable.commit("testview")
        state <- c.state.get
      } yield commitAttempts(state) must beEqualTo(
        Vector(
          Action.CommitAttempted("testview", isRetry = false),
          Action.CommitAttempted("testview", isRetry = true),
          Action.CommitAttempted("testview", isRetry = true)
        )
      )

      TestControl.executeEmbed(test)
    }
  }

  // A retry's lookup stops at the first other snapshot of this process's, so it is only right if no
  // other window of this process can commit between a window's attempts. A lock taken per attempt
  // would let the second window in during the first one's backoff, and consume the mock response
  // the first window's retry is waiting for.
  //
  // The second commit starts only once the first one's failing attempt has raised, so it always
  // reaches the lock while the first window still has its retry ahead of it.
  def e9 = {
    val mocks = Mocks(List(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val test = for {
        firstFailed <- Deferred[IO, Unit]
        remoteTable <-
          RemoteTable.build(signalFirstFailure(c.attempts, firstFailed), c.appHealth, retriesConfig, dummyDestinationSetupErrorCheck)
        first <- remoteTable.commit("window1").start
        _ <- firstFailed.get
        second <- remoteTable.commit("window2").start
        _ <- first.joinWithNever
        _ <- second.joinWithNever
        state <- c.state.get
      } yield commitAttempts(state) must beEqualTo(
        Vector(
          Action.CommitAttempted("window1", isRetry = false),
          Action.CommitAttempted("window1", isRetry = true),
          Action.CommitAttempted("window2", isRetry = false)
        )
      )

      TestControl.executeEmbed(test)
    }
  }

  def e10 =
    control(metricsFail = true).flatMap { c =>
      val test = for {
        remoteTable <- c.build(dummyDestinationSetupErrorCheck)
        dataFiles <- remoteTable.getTableDataFilesTotal
        snapshots <- remoteTable.getTableSnapshotsRetained
      } yield (dataFiles, snapshots) must beEqualTo((None, None))

      TestControl.executeEmbed(test)
    }
}

object RemoteTableSpec {

  sealed trait Action

  object Action {
    case object CreateTableAttempted extends Action
    case class CommitAttempted(viewName: String, isRetry: Boolean) extends Action
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

  case class Mocks(results: List[Response])

  case class Control(
    state: Ref[IO, Vector[Action]],
    attempts: RemoteTable.Attempts[IO],
    appHealth: AppHealth.Interface[IO, Alert, RuntimeService]
  ) {
    def build(destinationSetupErrorCheck: DestinationSetupErrorCheck): IO[RemoteTable[IO]] =
      RemoteTable.build(attempts, appHealth, retriesConfig, destinationSetupErrorCheck)
  }

  val retriesConfig = Config.Retries(
    Retrying.Config.ForSetup(30.seconds),
    Retrying.Config.ForTransient(1.second, 5)
  )

  /** Completes `failed` when a commit's first attempt raises */
  def signalFirstFailure(underlying: RemoteTable.Attempts[IO], failed: Deferred[IO, Unit]): RemoteTable.Attempts[IO] =
    new RemoteTable.Attempts[IO] {
      def create: IO[Unit] = underlying.create
      def attemptCommit(viewName: String, isRetry: Boolean): IO[Unit] =
        underlying.attemptCommit(viewName, isRetry).onError { case _ if !isRetry => failed.complete(()).void }
      def getTableDataFilesTotal: IO[Option[Long]]    = underlying.getTableDataFilesTotal
      def getTableSnapshotsRetained: IO[Option[Long]] = underlying.getTableSnapshotsRetained
    }

  def commitAttempts(state: Vector[Action]): Vector[Action] =
    state.collect { case a: Action.CommitAttempted => a }

  def control(mocks: Mocks = Mocks(Nil), metricsFail: Boolean = false): IO[Control] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      attempts <- testAttempts(state, mocks.results, metricsFail)
    } yield Control(state, attempts, testAppHealth(state))

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

  private def testAttempts(
    state: Ref[IO, Vector[Action]],
    mocks: List[Response],
    metricsFail: Boolean
  ): IO[RemoteTable.Attempts[IO]] =
    for {
      mocksRef <- Ref[IO].of(mocks)
    } yield new RemoteTable.Attempts[IO] {
      private def respond(action: Action): IO[Unit] =
        for {
          response <- mocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ action)
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

      def create: IO[Unit] =
        respond(Action.CreateTableAttempted)

      def attemptCommit(viewName: String, isRetry: Boolean): IO[Unit] =
        respond(Action.CommitAttempted(viewName, isRetry))

      def getTableDataFilesTotal: IO[Option[Long]] =
        if (metricsFail) IO.raiseError(new RuntimeException("boom reading data files")) else IO(Some(123L))

      def getTableSnapshotsRetained: IO[Option[Long]] =
        if (metricsFail) IO.raiseError(new RuntimeException("boom reading snapshots")) else IO(Some(456L))
    }
}
