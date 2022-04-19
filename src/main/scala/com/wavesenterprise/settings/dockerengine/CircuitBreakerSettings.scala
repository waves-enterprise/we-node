package com.wavesenterprise.settings.dockerengine

import cats.Show
import com.wavesenterprise.settings.{PositiveInt, WEConfigReaders}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.{Duration, FiniteDuration}

case class CircuitBreakerSettings(maxFailures: PositiveInt,
                                  contractOpeningLimit: Int,
                                  openedBreakersLimit: Int,
                                  expireAfter: FiniteDuration,
                                  resetTimeout: FiniteDuration,
                                  exponentialBackoffFactor: Double,
                                  maxResetTimeout: FiniteDuration) {
  require(exponentialBackoffFactor >= 1, "exponentialBackoffFactor param value should be >= 1")
  require(resetTimeout > Duration.Zero, "resetTimeout duration should be > 0")
  require(maxResetTimeout > Duration.Zero, "maxResetTimeout duration should be > 0")
  require(contractOpeningLimit >= 0, s"contractOpeningLimit should be >= 0")
  require(openedBreakersLimit >= 0, s"openedBreakersLimit should be >= 0")
}

object CircuitBreakerSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[CircuitBreakerSettings] = deriveReader

  implicit val toPrintable: Show[CircuitBreakerSettings] = { x =>
    import x._

    s"""
       |maxFailures: $maxFailures
       |contractOpeningLimit: $contractOpeningLimit
       |openedBreakersLimit: $openedBreakersLimit
       |expireAfter: $expireAfter
       |resetTimeout: $resetTimeout
       |exponentialBackoffFactor: $exponentialBackoffFactor
       |maxResetTimeout: $maxResetTimeout
       """.stripMargin
  }
}
