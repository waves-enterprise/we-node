package com.wavesenterprise.settings

import cats.Show
import cats.implicits.showInterpolator
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

case class AdditionalCacheSettings(rocksdb: RocksDBCacheSettings, blockIds: BlockIdsCacheSettings)

object AdditionalCacheSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[AdditionalCacheSettings] = deriveReader

  implicit val toPrintable: Show[AdditionalCacheSettings] = { x =>
    import x._
    s"""
       |rocksdb:
       |  ${show"$rocksdb".replace("\n", "\n--")}
       |blockIds:
       |  ${show"$blockIds".replace("\n", "\n--")}
     """.stripMargin
  }
}

case class RocksDBCacheSettings(maxCacheSize: Int, maxRollbackDepth: Int)

object RocksDBCacheSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[RocksDBCacheSettings] = deriveReader

  implicit val toPrintable: Show[RocksDBCacheSettings] = { x =>
    import x._
    s"""
       |maxCacheSize: $maxCacheSize
       |maxRollbackDepth: $maxRollbackDepth
     """.stripMargin
  }
}

case class BlockIdsCacheSettings(maxSize: Int, expireAfter: FiniteDuration)

object BlockIdsCacheSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[BlockIdsCacheSettings] = deriveReader

  implicit val toPrintable: Show[BlockIdsCacheSettings] = { x =>
    import x._
    s"""
       |maxSize: $maxSize
       |expireAfter: $expireAfter
     """.stripMargin
  }
}
