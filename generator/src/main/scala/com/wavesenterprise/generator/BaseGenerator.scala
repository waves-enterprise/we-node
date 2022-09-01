package com.wavesenterprise.generator
import com.typesafe.config.ConfigException
import com.wavesenterprise.utils.LoggerFacade
import monix.eval.Task
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

trait BaseGenerator[T] {
  val log: LoggerFacade = LoggerFacade(LoggerFactory.getLogger(this.getClass))
  log.info("Generator started...")

  final def main(args: Array[String]): Unit = {
    val scheduler = monix.execution.Scheduler(scala.concurrent.ExecutionContext.global)

    val f = generateFlow(args)
      .doOnFinish { maybeError =>
        Task {
          maybeError match {
            case Some(th) =>
              log.error("Generator failed...")
              combinedExceptionHandler(th)
              close(1)
            case None =>
              log.info("Generator done")
              close(0)
          }
        }
      }
      .runToFuture(scheduler)

    Await.result(f, Duration.Inf)
  }

  def generateFlow(args: Array[String]): Task[T]

  def exceptionHandlers: PartialFunction[Throwable, Unit]

  private val defaultExceptionHandlers: PartialFunction[Throwable, Unit] = {
    case ex: ConfigException =>
      log.error(s"Failed to parse config, message: '${ex.getMessage}'")

    case ex: Throwable =>
      log.error(s"Encountered a failure, message: ${ex.getMessage}. Exception class: ${ex.getClass.getSimpleName}")
  }

  private def combinedExceptionHandler: PartialFunction[Throwable, Unit] =
    exceptionHandlers.orElse(defaultExceptionHandlers)

  def internalClose(): Unit

  def close(status: Int): Unit = {
    internalClose()
    System.exit(status)
  }
}
