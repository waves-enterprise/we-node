package com.wavesenterprise.settings.privacy

import cats.Show
import com.wavesenterprise.settings.{PositiveInt, WEConfigReaders}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.{Duration, FiniteDuration}

case class PrivacySynchronizerSettings(
    requestTimeout: FiniteDuration,
    initRetryDelay: FiniteDuration,
    inventoryStreamTimeout: FiniteDuration,
    inventoryRequestDelay: FiniteDuration,
    inventoryTimestampThreshold: FiniteDuration,
    crawlingParallelism: PositiveInt,
    maxAttemptCount: PositiveInt,
    lostDataProcessingDelay: FiniteDuration,
    networkStreamBufferSize: PositiveInt,
    failedPeersCacheSize: Int,
    failedPeersCacheExpireTimeout: FiniteDuration
) {
  require(requestTimeout > Duration.Zero, "RequestTimeout must be positive")
  require(initRetryDelay > Duration.Zero, "InitRetryDelay must be positive")
  require(inventoryStreamTimeout > Duration.Zero, "InventoryStreamTimeout must be positive")
  require(inventoryRequestDelay > Duration.Zero, "InventoryRequestDelay must be positive")
  require(inventoryTimestampThreshold > Duration.Zero, "InventoryTimestampThreshold must be positive")
  require(lostDataProcessingDelay > Duration.Zero, "LostDataProcessingDelay must be positive")
}

object PrivacySynchronizerSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[PrivacySynchronizerSettings] = deriveReader

  implicit val toPrintable: Show[PrivacySynchronizerSettings] = { x =>
    import x._
    s"""
       |requestTimeout: $requestTimeout
       |initRetryDelay: $initRetryDelay
       |inventoryStreamTimeout: $inventoryStreamTimeout
       |inventoryRequestDelay: $inventoryRequestDelay
       |inventoryTimestampThreshold: $inventoryTimestampThreshold
       |crawlingParallelism: ${crawlingParallelism.value}
       |maxAttemptCount: ${maxAttemptCount.value}
       |lostDataProcessingDelay: $lostDataProcessingDelay
       |networkStreamBufferSize: ${networkStreamBufferSize.value}
       |failedPeersCacheSize: $failedPeersCacheSize
       |failedPeersCacheExpireTimeout: $failedPeersCacheExpireTimeout
       """.stripMargin
  }
}
