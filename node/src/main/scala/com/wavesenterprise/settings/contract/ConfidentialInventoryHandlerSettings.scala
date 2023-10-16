package com.wavesenterprise.settings.contract

import cats.Show
import com.wavesenterprise.settings.{PositiveInt, WEConfigReaders}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

case class ConfidentialInventoryHandlerSettings(
    maxBufferTime: FiniteDuration,
    maxBufferSize: PositiveInt,
    maxCacheSize: PositiveInt,
    expirationTime: FiniteDuration,
    replierParallelism: PositiveInt
)

object ConfidentialInventoryHandlerSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[ConfidentialInventoryHandlerSettings] = deriveReader

  implicit val toPrintable: Show[ConfidentialInventoryHandlerSettings] = { x =>
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
