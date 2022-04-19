package com.wavesenterprise.settings.privacy

import cats.Show
import com.wavesenterprise.settings.{PositiveInt, WEConfigReaders}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import pureconfig.module.squants._
import squants.information.Information

import scala.concurrent.duration.{Duration, FiniteDuration}

case class PrivacyReplierSettings(parallelism: PositiveInt,
                                  streamTimeout: FiniteDuration,
                                  streamChunkSize: Information,
                                  streamMaxPeersFullnessPercentage: PositiveInt) {
  require(streamTimeout > Duration.Zero, "StreamTimeout must be positive")
  require(streamChunkSize.toBytes > 0, "StreamChunkSize must be positive")
  require(streamMaxPeersFullnessPercentage.value <= 100, "StreamMaxPeersFullnessPercentage should not exceed 100")
}

object PrivacyReplierSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[PrivacyReplierSettings] = deriveReader

  implicit val toPrintable: Show[PrivacyReplierSettings] = { x =>
    import x._
    s"""
       |parallelism: ${parallelism.value}
       |streamTimeout: $streamTimeout
       |streamChunkSize: $streamChunkSize
       |streamMaxPeersFullnessPercentage: $streamMaxPeersFullnessPercentage
       """.stripMargin
  }
}
