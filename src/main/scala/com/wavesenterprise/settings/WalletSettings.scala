package com.wavesenterprise.settings

import java.io.File

import pureconfig.generic.semiauto._
import cats.Show
import pureconfig.ConfigReader

case class WalletSettings(file: Option[File], password: String)

object WalletSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[WalletSettings] = deriveReader

  implicit val toPrintable: Show[WalletSettings] = { x =>
    import x._
    s"""
       |file: $file
       |password: $password
     """.stripMargin
  }
}
