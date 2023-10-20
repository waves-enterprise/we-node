package com.wavesenterprise.settings.contract

import cats.Show
import com.wavesenterprise.settings.{PositiveInt, WEConfigReaders}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class ConfidentialDataReplierSettings(
    parallelism: PositiveInt
)

object ConfidentialDataReplierSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[ConfidentialDataReplierSettings] = deriveReader

  implicit val toPrintable: Show[ConfidentialDataReplierSettings] = { s =>
    import s._
    s"""
       |parallelism: ${parallelism.value}
       |""".stripMargin
  }
}
