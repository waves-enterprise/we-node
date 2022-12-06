package com.wavesenterprise.docker

import com.google.common.cache.{CacheBuilder, RemovalNotification}
import com.wavesenterprise.docker.CircuitBreakerMetrics._
import com.wavesenterprise.metrics.Metrics.CircuitBreakerCacheSettings
import kamon.Kamon
import kamon.metric.{Counter, CounterMetric, Timer}

import java.util.concurrent.TimeUnit

trait CircuitBreakerMetrics {

  protected def circuitBreakerCacheSettings: CircuitBreakerCacheSettings

  protected def openStateCounter: CounterMetric     = Kamon.counter("docker-circuit-breaker.open")
  protected def halfOpenStateCounter: CounterMetric = Kamon.counter("docker-circuit-breaker.half-open")
  protected def closedStateCounter: CounterMetric   = Kamon.counter("docker-circuit-breaker.closed")
  protected def rejectedStateCounter: CounterMetric = Kamon.counter("docker-circuit-breaker.rejected")

  private val counterCache = CacheBuilder
    .newBuilder()
    .maximumSize(circuitBreakerCacheSettings.maxSize)
    .expireAfterWrite(circuitBreakerCacheSettings.expireAfter.toMillis, TimeUnit.MILLISECONDS)
    .removalListener((notification: RemovalNotification[(CircuitBreakerState, String), Counter]) => {
      val (state, image) = notification.getKey

      state match {
        case CircuitBreakerState.Open =>
          openStateCounter.remove("image" -> image)
        case CircuitBreakerState.HalfOpen =>
          halfOpenStateCounter.remove("image" -> image)
        case CircuitBreakerState.Closed =>
          closedStateCounter.remove("image" -> image)
        case CircuitBreakerState.Rejected =>
          rejectedStateCounter.remove("image" -> image)
      }
    })
    .build[(CircuitBreakerState, String), Counter]

  protected def reportStateChange(state: CircuitBreakerState, image: String): Unit = {

    val counter = counterCache.get(
      (state, image), { () =>
        state match {
          case CircuitBreakerState.Open =>
            openStateCounter.refine("image" -> image)
          case CircuitBreakerState.HalfOpen =>
            halfOpenStateCounter.refine("image" -> image)
          case CircuitBreakerState.Closed =>
            closedStateCounter.refine("image" -> image)
          case CircuitBreakerState.Rejected =>
            rejectedStateCounter.refine("image" -> image)
        }
      }
    )

    counter.increment()
  }

}

object CircuitBreakerMetrics {
  sealed trait CircuitBreakerState

  object CircuitBreakerState {
    case object Open     extends CircuitBreakerState
    case object HalfOpen extends CircuitBreakerState
    case object Closed   extends CircuitBreakerState
    case object Rejected extends CircuitBreakerState
  }

}
