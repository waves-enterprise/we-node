package com.wavesenterprise.settings.api

import cats.Show
import com.wavesenterprise.settings.WEConfigReaders
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class ContractStatusServiceSettings(maxConnections: Int) extends MaxConnections

object ContractStatusServiceSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[ContractStatusServiceSettings] = deriveReader

  implicit val toPrintable: Show[ContractStatusServiceSettings] = { x =>
    import x._
    s"""
       |maxConnections:$maxConnections
     """.stripMargin
  }
}
