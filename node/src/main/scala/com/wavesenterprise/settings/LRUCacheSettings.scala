package com.wavesenterprise.settings

import cats.Show
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

case class LRUCacheSettings(maxSize: Int, expireAfter: FiniteDuration)

object LRUCacheSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[LRUCacheSettings] = deriveReader

  implicit val toPrintable: Show[LRUCacheSettings] = { x =>
    import x._
    s"""
       |maxSize: $maxSize
       |expireAfter: $expireAfter
       """.stripMargin
  }
}
