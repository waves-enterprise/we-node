package com.wavesenterprise.settings.privacy

import cats.Show
import com.wavesenterprise.settings.{PositiveInt, WEConfigReaders}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

case class PrivacyInventoryHandlerSettings(
    maxBufferTime: FiniteDuration,
    maxBufferSize: PositiveInt,
    maxCacheSize: PositiveInt,
    expirationTime: FiniteDuration,
    replierParallelism: PositiveInt
)

object PrivacyInventoryHandlerSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[PrivacyInventoryHandlerSettings] = deriveReader

  implicit val toPrintable: Show[PrivacyInventoryHandlerSettings] = { x =>
    import x._
    s"""
       |maxBufferTime: $maxBufferTime
       |maxBufferSize: ${maxBufferSize.value}
       |maxCacheSize: ${maxCacheSize.value}
       |expirationTime: $expirationTime
       |replierParallelism: ${replierParallelism.value}
       """.stripMargin
  }
}
