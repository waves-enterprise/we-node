package com.wavesenterprise.settings.api

import cats.Show
import cats.syntax.show._
import com.typesafe.config.Config
import com.wavesenterprise.settings.WEConfigReaders
import com.wavesenterprise.utils.StringUtils.dashes
import scala.util.chaining.scalaUtilChainingOps
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class GrpcApiSettings(
    enable: Boolean,
    bindAddress: String,
    port: Int,
    akkaHttpSettings: Config,
    services: ServicesSettings
)

object GrpcApiSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[GrpcApiSettings] = deriveReader

  implicit val toPrintable: Show[GrpcApiSettings] = { x =>
    import x._

    s"""
       |enable: $enable
       |bindAddress: $bindAddress
       |port: $port
       |services: ${services.show pipe dashes}
       |akka-http-settings: $akkaHttpSettings
     """.stripMargin
  }
}
