package com.wavesenterprise.settings.wasm

import cats.Show
import com.wavesenterprise.settings.WEConfigReaders
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

case class WASMSettings(
    fuelLimit: Long
)

object WASMSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[WASMSettings] = deriveReader

  implicit val toPrintable: Show[WASMSettings] = { x =>
    import x._
    s"fuel-limit: $fuelLimit"
  }
}
