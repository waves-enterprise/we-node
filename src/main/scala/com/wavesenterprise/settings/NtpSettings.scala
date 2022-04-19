package com.wavesenterprise.settings

import cats.Show
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration._

case class NtpSettings(servers: Seq[String], requestTimeout: FiniteDuration, expirationTimeout: FiniteDuration, fatalTimeout: Option[FiniteDuration])

object NtpSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[NtpSettings] = deriveReader

  implicit val toPrintable: Show[NtpSettings] = { settings =>
    import settings._

    s"""
       |servers: ${servers.mkString(", ")}
       |request-timeout: $requestTimeout
       |expiration-timeout: $expirationTimeout
       |fatal-timeout: ${fatalTimeout.getOrElse("undefined")}
     """.stripMargin
  }
}
