package com.wavesenterprise.settings

import cats.Show
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class RestApiSettings(
    enable: Boolean,
    bindAddress: String,
    port: Int,
    cors: Boolean,
    transactionsByAddressLimit: Int,
    distributionAddressLimit: Int
)

object RestApiSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[RestApiSettings] = deriveReader

  implicit val toPrintable: Show[RestApiSettings] = { x =>
    import x._

    s"""
       |enable: $enable
       |bindAddress: $bindAddress
       |port: $port
       |cors: $cors
       |transactionByAddressLimit: $transactionsByAddressLimit
       |distributionAddressLimit: $distributionAddressLimit
     """.stripMargin
  }
}
