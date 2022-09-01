package com.wavesenterprise.settings.privacy

import cats.Show
import com.wavesenterprise.settings.WEConfigReaders
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

case class PolicyDataCacheSettings(maxSize: Int, expireAfter: FiniteDuration)

object PolicyDataCacheSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[PolicyDataCacheSettings] = deriveReader

  implicit val toPrintable: Show[PolicyDataCacheSettings] = { x =>
    import x._
    s"""
       |maxSize: $maxSize
       |expireAfter: $expireAfter
       """.stripMargin
  }
}
