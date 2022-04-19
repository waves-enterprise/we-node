package com.wavesenterprise.settings.dockerengine

import cats.Show
import com.wavesenterprise.settings.{PositiveInt, WEConfigReaders}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/**
  * Settings for [[com.wavesenterprise.docker.ContractExecutionMessagesCache]]
  */
case class ContractExecutionMessagesCacheSettings(
    expireAfter: FiniteDuration,
    maxSize: Int,
    maxBufferSize: Int,
    maxBufferTime: FiniteDuration,
    utxCleanupInterval: FiniteDuration,
    contractErrorQuorum: PositiveInt,
    ignoreSuccessful: Boolean = false
)

object ContractExecutionMessagesCacheSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[ContractExecutionMessagesCacheSettings] = deriveReader

  implicit val toPrintable: Show[ContractExecutionMessagesCacheSettings] = { x =>
    import x._

    s"""
       |maxSize: $maxSize
       |expireAfter: $expireAfter
       |maxBuffersSize: $maxBufferSize
       |maxBufferTime: $maxBufferTime
       |utxCleanupInterval: $utxCleanupInterval
       |contractErrorQuorum: ${contractErrorQuorum.value}
       |ignoreSuccessful: $ignoreSuccessful
       """.stripMargin
  }
}
