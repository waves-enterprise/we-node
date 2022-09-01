package com.wavesenterprise.settings.privacy

import cats.Show
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import pureconfig.module.squants._
import squants.information.Information

import scala.concurrent.duration.{Duration, FiniteDuration}

case class PrivacyServiceSettings(requestBufferSize: Information, metaDataAccumulationTimeout: FiniteDuration) {
  require(requestBufferSize.toBytes > 0, "multipartRequestBufferSize must be positive")
  require(metaDataAccumulationTimeout > Duration.Zero, "metaDataAccumulationTimeout must be > 0")
}

object PrivacyServiceSettings {
  implicit val configReader: ConfigReader[PrivacyServiceSettings] = deriveReader

  implicit val toPrintable: Show[PrivacyServiceSettings] = { x =>
    s"""|requestBufferSize: ${x.requestBufferSize}
        |metaDataAccumulationTimeout: ${x.metaDataAccumulationTimeout}
     """.stripMargin
  }
}
