package com.wavesenterprise.docker

import cats.implicits._
import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalNotification}
import com.wavesenterprise.docker.CircuitBreakerSupport.CircuitBreakerError.{ContractOpeningLimitError, OpenedCircuitBreakersLimitError}
import com.wavesenterprise.docker.ContractExecutor.ContainerKey
import com.wavesenterprise.settings.dockerengine.CircuitBreakerSettings
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ScorexLogging
import monix.catnap.CircuitBreaker
import monix.eval.Task
import monix.execution.atomic.AtomicInt

import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

trait CircuitBreakerSupport extends ScorexLogging {

  private[this] val circuitBreakers =
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(circuitBreakerSettings.expireAfter.toMillis, TimeUnit.MILLISECONDS)
      .removalListener { (notification: RemovalNotification[ContainerKey, Task[CircuitBreaker[Task]]]) =>
        contractOpeningCounters.invalidate(notification.getKey)
      }
      .build[ContainerKey, Task[CircuitBreaker[Task]]](new CacheLoader[ContainerKey, Task[CircuitBreaker[Task]]] {
        override def load(key: ContainerKey): Task[CircuitBreaker[Task]] = newCircuitBreaker(key)
      })

  private[this] val contractOpeningCounters =
    CacheBuilder
      .newBuilder()
      .removalListener { (notification: RemovalNotification[ContainerKey, AtomicInt]) =>
        if (notification.getValue.get > 0) openedCircuitBreakersCounter.decrement()
      }
      .build[ContainerKey, AtomicInt](new CacheLoader[ContainerKey, AtomicInt] {
        override def load(key: ContainerKey): AtomicInt = AtomicInt(0)
      })

  private[this] val openedCircuitBreakersCounter = AtomicInt(0)

  protected def circuitBreakerSettings: CircuitBreakerSettings

  private def newCircuitBreaker(containerKey: ContainerKey): Task[CircuitBreaker[Task]] = {
    val CircuitBreakerSettings(maxFailures, _, _, _, resetTimeout, exponentialBackoffFactor, maxResetTimeout) = circuitBreakerSettings
    CircuitBreaker[Task]
      .of(
        maxFailures = maxFailures.value,
        resetTimeout = resetTimeout,
        exponentialBackoffFactor = exponentialBackoffFactor,
        maxResetTimeout = maxResetTimeout,
        onOpen = Task {
          log.debug(s"Switched to Open, all incoming calls rejected for container with image '${containerKey.image}'")
          if (contractOpeningCounters.get(containerKey).incrementAndGet() == 1) openedCircuitBreakersCounter.increment()
        },
        onHalfOpen = Task {
          log.debug(s"Switched to HalfOpen, accepted one call for testing container with image '${containerKey.image}'")
        },
        onClosed = Task {
          log.debug(s"Switched to Close, accepting calls again to container with image '${containerKey.image}'")
          contractOpeningCounters.invalidate(containerKey)
        }
      )
      .memoizeOnSuccess
  }

  protected def protect[A](contract: ContractInfo)(task: Task[A]): Task[A] = {
    val containerKey = ContainerKey(contract)
    for {
      circuitBreaker <- circuitBreakers.get(containerKey)
      result <- circuitBreaker
        .protect(task)
        .onErrorRecoverWith {
          case NonFatal(ex) if openedCircuitBreakersCounter.get > circuitBreakerSettings.openedBreakersLimit =>
            Task.raiseError(OpenedCircuitBreakersLimitError(openedCircuitBreakersCounter.get, ex))
          case NonFatal(ex) if contractOpeningCounters.get(containerKey).get > circuitBreakerSettings.contractOpeningLimit =>
            Task {
              contractOpeningCounters.get(containerKey).get
            } >>= { count =>
              Task(contractOpeningCounters.invalidate(containerKey)) *> Task.raiseError(ContractOpeningLimitError(contract.contractId, count, ex))
            }
        }
    } yield result
  }
}

object CircuitBreakerSupport {

  sealed abstract class CircuitBreakerError(val message: String, val cause: Throwable) extends RuntimeException(message, cause)

  object CircuitBreakerError {

    case class OpenedCircuitBreakersLimitError(openBreakersCount: Int, override val cause: Throwable)
        extends CircuitBreakerError(s"Too many opened circuit breakers '$openBreakersCount', cause: $cause", cause)

    case class ContractOpeningLimitError(contractId: ByteStr, openCount: Int, override val cause: Throwable)
        extends CircuitBreakerError(s"Too many contract '$contractId' circuit breaker open events '$openCount', cause: $cause", cause)
  }
}
