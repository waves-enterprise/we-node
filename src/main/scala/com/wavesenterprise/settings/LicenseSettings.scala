package com.wavesenterprise.settings

import java.io.File

import cats.Show
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration._

case class LicenseSettings(
    file: File,
    demoHeight: Int = LicenseSettings.MaxDemoHeight,
    checkInterval: FiniteDuration = LicenseSettings.CheckIntervalDefault
)

object LicenseSettings extends WEConfigReaders {
  val MaxDemoHeight: Int                                   = 30000
  val CheckIntervalRange: (FiniteDuration, FiniteDuration) = (5 seconds, 1 hour)
  val CheckIntervalDefault: FiniteDuration                 = 10 minutes

  implicit val configReader: ConfigReader[LicenseSettings] = deriveReader[LicenseSettings].map { licenseSettings =>
    licenseSettings.copy(
      demoHeight = licenseSettings.demoHeight.min(MaxDemoHeight),
      checkInterval = Some(licenseSettings.checkInterval)
        .filter(fd => fd >= CheckIntervalRange._1 && fd <= CheckIntervalRange._2)
        .getOrElse(CheckIntervalDefault)
    )
  }

  implicit val toPrintable: Show[LicenseSettings] = { s =>
    import s._

    s"""
       |file: $file
       |demo-height: $demoHeight
       |check-interval: $checkInterval
     """.stripMargin
  }
}
