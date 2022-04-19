package com.wavesenterprise.settings

import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource

import scala.concurrent.duration.DurationInt

class HealthCheckSettingsSpec extends FlatSpec with Matchers {
  "HealthCheckEnabledSettings" should "read config string correctly" in {
    val config = ConfigSource.string {
      """
        |enable = true
        |interval = 20s
        |timeout = 10s""".stripMargin
    }

    val healthCheckSettings = config.loadOrThrow[HealthCheckSettings]
    healthCheckSettings shouldBe a[HealthCheckEnabledSettings]
    val enabledSettings = healthCheckSettings.asInstanceOf[HealthCheckEnabledSettings]
    enabledSettings.interval shouldBe 20.seconds
    enabledSettings.timeout shouldBe 10.seconds
  }

  "HealthCheckDisabledSettings" should "read config string correctly" in {
    val config = ConfigSource.string {
      """
        |enable = false
      """.stripMargin
    }

    val healthCheckSettings = config.loadOrThrow[HealthCheckSettings]
    healthCheckSettings shouldBe a[HealthCheckDisabledSettings.type]
  }

  "interval lower than timeout" should "not pass" in {
    val config = ConfigSource.string {
      """
        |enable = true
        |interval = 9s
        |timeout = 10s""".stripMargin
    }
    val expectedMessage = "requirement failed: Node health check interval should be greater or equal to timeout"

    the[RuntimeException] thrownBy config.loadOrThrow[HealthCheckEnabledSettings] should have message expectedMessage
  }

  "timeout lower than 5 seconds" should "not pass" in {
    val config = ConfigSource.string {
      """
        |enable = true
        |interval = 9s
        |timeout = 3s""".stripMargin
    }
    val expectedMessage = "requirement failed: Node health check timeout should be at least 5 seconds"

    the[RuntimeException] thrownBy config.loadOrThrow[HealthCheckEnabledSettings] should have message expectedMessage
  }
}
