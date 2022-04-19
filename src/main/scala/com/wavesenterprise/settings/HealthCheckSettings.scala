package com.wavesenterprise.settings

import cats.Show
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import scala.concurrent.duration.DurationInt

import scala.concurrent.duration.FiniteDuration

sealed trait HealthCheckSettings {
  def enable: Boolean
}

object HealthCheckSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[HealthCheckSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      enableCursor <- objectCursor.atKey("enable")
      isEnable     <- enableCursor.asBoolean
      settings     <- if (isEnable) HealthCheckEnabledSettings.configReader.from(objectCursor) else Right(HealthCheckDisabledSettings)
    } yield settings
  }

  implicit val toPrintable: Show[HealthCheckSettings] = {
    case HealthCheckDisabledSettings => "enable: false"
    case s: HealthCheckEnabledSettings =>
      s"""
         |enable: true
         |interval: ${s.interval}
         |timeout: ${s.timeout}
       """.stripMargin
  }
}

object HealthCheckDisabledSettings extends HealthCheckSettings {
  override def enable: Boolean = false
}

case class HealthCheckEnabledSettings(interval: FiniteDuration, timeout: FiniteDuration) extends HealthCheckSettings {
  require(interval >= timeout, "Node health check interval should be greater or equal to timeout")
  require(timeout >= 5.seconds, "Node health check timeout should be at least 5 seconds")
  override def enable: Boolean = true
}

object HealthCheckEnabledSettings {
  implicit val configReader: ConfigReader[HealthCheckEnabledSettings] = deriveReader
}
