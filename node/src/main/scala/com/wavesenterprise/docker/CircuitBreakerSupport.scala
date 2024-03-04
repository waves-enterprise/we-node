package com.wavesenterprise.docker

import cats.implicits._
import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalNotification}
import com.wavesenterprise.docker.CircuitBreakerMetrics.CircuitBreakerState
import com.wavesenterprise.docker.CircuitBreakerSupport.CircuitBreakerError.{ContractOpeningLimitError, OpenedCircuitBreakersLimitError}
import com.wavesenterprise.docker.StoredContract.DockerContract
import com.wavesenterprise.docker.DockerContractExecutor.ContainerKey
import com.wavesenterprise.docker.exceptions.FatalExceptionsMatchers._
import com.wavesenterprise.settings.dockerengine.CircuitBreakerSettings
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.atomic.AtomicInt

import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

trait CircuitBreakerSupport extends CircuitBreakerMetrics with ScorexLogging {

  private[this] val circuitBreakers =
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(circuitBreakerSettings.expireAfter.toMillis, TimeUnit.MILLISECONDS)
      .removalListener { (notification: RemovalNotification[ContainerKey, Task[CircuitBreakerWithExceptions[Task]]]) =>
        contractOpeningCounters.invalidate(notification.getKey)
      }
      .build[ContainerKey, Task[CircuitBreakerWithExceptions[Task]]](new CacheLoader[ContainerKey, Task[CircuitBreakerWithExceptions[Task]]] {
        override def load(key: ContainerKey): Task[CircuitBreakerWithExceptions[Task]] = newCircuitBreaker(key)
      })

  private[this] val circuitTxBreakers =
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(circuitBreakerSettings.expireAfter.toMillis, TimeUnit.MILLISECONDS)
      .removalListener { (notification: RemovalNotification[ByteStr, Task[CircuitBreakerWithExceptions[Task]]]) =>
        txOpeningCounters.invalidate(notification.getKey)
      }
      .build[ByteStr, Task[CircuitBreakerWithExceptions[Task]]](new CacheLoader[ByteStr, Task[CircuitBreakerWithExceptions[Task]]] {
        override def load(key: ByteStr): Task[CircuitBreakerWithExceptions[Task]] = newAtomicCircuitBreaker(key)
      })

  private[this] val txOpeningCounters =
    CacheBuilder
      .newBuilder()
      .removalListener { (notification: RemovalNotification[ByteStr, AtomicInt]) =>
        if (notification.getValue.get > 0) openedTxCircuitBreakersCounter.decrement()
      }
      .build[ByteStr, AtomicInt](new CacheLoader[ByteStr, AtomicInt] {
        override def load(key: ByteStr): AtomicInt = AtomicInt(0)
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

  private[this] val openedCircuitBreakersCounter   = AtomicInt(0)
  private[this] val openedTxCircuitBreakersCounter = AtomicInt(0)

  protected def circuitBreakerSettings: CircuitBreakerSettings

  private def newCircuitBreaker(containerKey: ContainerKey): Task[CircuitBreakerWithExceptions[Task]] = {
    val CircuitBreakerSettings(maxFailures, _, _, _, resetTimeout, exponentialBackoffFactor, maxResetTimeout) = circuitBreakerSettings
    CircuitBreakerWithExceptions[Task]
      .of(
        maxFailures = maxFailures.value,
        resetTimeout = resetTimeout,
        exponentialBackoffFactor = exponentialBackoffFactor,
        maxResetTimeout = maxResetTimeout,
        onOpen = Task {
          reportStateChange(CircuitBreakerState.Open, containerKey.image)
          log.debug(s"Switched to Open, all incoming calls rejected for container with image '${containerKey.image}'")
          if (contractOpeningCounters.get(containerKey).incrementAndGet() == 1) openedCircuitBreakersCounter.increment()
        },
        onHalfOpen = Task {
          reportStateChange(CircuitBreakerState.HalfOpen, containerKey.image)
          log.debug(s"Switched to HalfOpen, accepted one call for testing container with image '${containerKey.image}'")
        },
        onClosed = Task {
          reportStateChange(CircuitBreakerState.Closed, containerKey.image)
          log.debug(s"Switched to Close, accepting calls again to container with image '${containerKey.image}'")
          contractOpeningCounters.invalidate(containerKey)
        },
        onRejected = Task {
          reportStateChange(CircuitBreakerState.Rejected, containerKey.image)
        }
      )
      .memoizeOnSuccess
  }

  private def newAtomicCircuitBreaker(txId: ByteStr): Task[CircuitBreakerWithExceptions[Task]] = {
    val CircuitBreakerSettings(maxFailures, _, _, _, resetTimeout, exponentialBackoffFactor, maxResetTimeout) = circuitBreakerSettings
    CircuitBreakerWithExceptions[Task]
      .of(
        maxFailures = maxFailures.value,
        resetTimeout = resetTimeout,
        exponentialBackoffFactor = exponentialBackoffFactor,
        maxResetTimeout = maxResetTimeout,
        onOpen = Task {
          reportStateChange(CircuitBreakerState.Open, txId.base58)
          log.debug(s"Switched to Open, all incoming calls rejected for container with image '$txId'")
          if (txOpeningCounters.get(txId).incrementAndGet() == 1) openedTxCircuitBreakersCounter.increment()
        },
        onHalfOpen = Task {
          reportStateChange(CircuitBreakerState.HalfOpen, txId.base58)
          log.debug(s"Switched to HalfOpen, accepted one call for testing container with image '$txId'")
        },
        onClosed = Task {
          reportStateChange(CircuitBreakerState.Closed, txId.base58)
          log.debug(s"Switched to Close, accepting calls again to container with image '$txId'")
          txOpeningCounters.invalidate(txId)
        },
        onRejected = Task {
          reportStateChange(CircuitBreakerState.Rejected, txId.base58)
        }
      )
      .memoizeOnSuccess
  }

  protected def protect[A](contract: ContractInfo, checkFatalExceptions: ExceptionsMatcher)(task: Task[A]): Task[A] = {
    val image        = contract.storedContract.asInstanceOf[DockerContract]
    val containerKey = ContainerKey(image.imageHash, image.image)
    for {
      circuitBreaker <- circuitBreakers.get(containerKey)
      result <- circuitBreaker
        .protectWithExceptions(task, checkFatalExceptions)
        .onErrorRecoverWith {
          case NonFatal(ex) if checkFatalExceptions(ex) =>
            Task.raiseError(ex)
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

  protected def protect[A](contract: ContractInfo, txId: ByteStr, checkFatalExceptions: ExceptionsMatcher)(task: Task[A]): Task[A] = {
    for {
      circuitBreaker <- circuitTxBreakers.get(txId)
      result <- circuitBreaker
        .protectWithExceptions(task, checkFatalExceptions)
        .onErrorRecoverWith {
          case NonFatal(ex) if checkFatalExceptions(ex) =>
            Task.raiseError(ex)
          case NonFatal(ex) if openedTxCircuitBreakersCounter.get > circuitBreakerSettings.openedBreakersLimit =>
            Task.raiseError(OpenedCircuitBreakersLimitError(openedTxCircuitBreakersCounter.get, ex))
          case NonFatal(ex) if txOpeningCounters.get(txId).get > circuitBreakerSettings.contractOpeningLimit =>
            Task {
              txOpeningCounters.get(txId).get
            } >>= { count =>
              Task(txOpeningCounters.invalidate(txId)) *> Task.raiseError(ContractOpeningLimitError(contract.contractId, count, ex))
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
