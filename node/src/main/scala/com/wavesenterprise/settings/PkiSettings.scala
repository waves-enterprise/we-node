package com.wavesenterprise.settings

import cats.Show
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

final case class PkiSettings(tspServer: String)

object PkiSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[PkiSettings] = deriveReader

  implicit val toPrintable: Show[PkiSettings] = { x =>
    import x._

    s"""
       |tspServer: $tspServer
     """.stripMargin
  }
}
