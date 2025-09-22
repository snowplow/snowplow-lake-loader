/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import cats.implicits._
import cats.effect.implicits._
import cats.effect.{Async, Deferred, ExitCode, Resource, Sync}
import cats.effect.std.Dispatcher
import cats.data.EitherT
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.circe.Decoder
import com.monovore.decline.Opts
import sun.misc.{Signal, SignalHandler}

import com.snowplowanalytics.snowplow.streams.Factory
import com.snowplowanalytics.snowplow.lakes.processing.Processing
import com.snowplowanalytics.snowplow.runtime.{AppInfo, ConfigParser, LogUtils, Telemetry}

import java.nio.file.Path

object Run {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def fromCli[F[_]: Async, FactoryConfig: Decoder, SourceConfig: Decoder, SinkConfig: Decoder](
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, SinkConfig]],
    destinationSetupErrorCheck: DestinationSetupErrorCheck
  ): Opts[F[ExitCode]] = {
    val configPathOpt = Opts.option[Path]("config", help = "path to config file")
    val igluPathOpt   = Opts.option[Path]("iglu-config", help = "path to iglu resolver config file")
    (configPathOpt, igluPathOpt).mapN { case (configPath, igluPath) =>
      fromConfigPaths(appInfo, toFactory, destinationSetupErrorCheck, configPath, igluPath)
        .race(waitForSignal)
        .map(_.merge)
    }
  }

  def fromConfigPaths[F[_]: Async, FactoryConfig: Decoder, SourceConfig: Decoder, SinkConfig: Decoder](
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, SinkConfig]],
    destinationSetupErrorCheck: DestinationSetupErrorCheck,
    pathToConfig: Path,
    pathToResolver: Path
  ): F[ExitCode] = {

    val eitherT = for {
      config <- ConfigParser.configFromFile[F, Config[FactoryConfig, SourceConfig, SinkConfig]](pathToConfig)
      resolver <- ConfigParser.igluResolverFromFile(pathToResolver)
      configWithIglu = Config.WithIglu(config, resolver)
      _ <- EitherT.right[String](fromConfig(appInfo, toFactory, destinationSetupErrorCheck, configWithIglu))
    } yield ExitCode.Success

    eitherT
      .leftSemiflatMap { s: String =>
        Logger[F].error(s).as(ExitCode.Error)
      }
      .merge
      .handleErrorWith { e =>
        Logger[F].error(e)("Exiting") >>
          LogUtils.prettyLogException(e).as(ExitCode.Error)
      }
  }

  private def fromConfig[F[_]: Async, FactoryConfig, SourceConfig, SinkConfig](
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, SinkConfig]],
    destinationSetupErrorCheck: DestinationSetupErrorCheck,
    config: Config.WithIglu[FactoryConfig, SourceConfig, SinkConfig]
  ): F[ExitCode] =
    Environment.fromConfig(config, appInfo, toFactory, destinationSetupErrorCheck).use { env =>
      Processing
        .stream(env)
        .concurrently(Telemetry.stream(config.main.telemetry, env.appInfo, env.httpClient))
        .concurrently(env.metrics.report)
        .compile
        .drain
        .as(ExitCode.Success)
    }

  /**
   * Trap the SIGTERM and begin graceful shutdown
   *
   * This is needed to prevent 3rd party libraries from starting their own shutdown before we are
   * ready
   */
  private def waitForSignal[F[_]: Async]: F[ExitCode] =
    Dispatcher.sequential(await = true).use { dispatcher =>
      for {
        deferred <- Deferred[F, Int]
        _ <- addShutdownHook(deferred, dispatcher)
        signal <- deferred.get
      } yield ExitCode(128 + signal)
    }

  private def addShutdownHook[F[_]: Sync](deferred: Deferred[F, Int], dispatcher: Dispatcher[F]): F[Unit] =
    Sync[F].delay {
      val handler = new SignalHandler {
        override def handle(signal: Signal): Unit =
          dispatcher.unsafeRunAndForget {
            Logger[F].info(s"Received signal ${signal.getNumber}. Cancelling execution.") *>
              deferred.complete(signal.getNumber)
          }
      }
      Signal.handle(new Signal("TERM"), handler)
      ()
    }

}
