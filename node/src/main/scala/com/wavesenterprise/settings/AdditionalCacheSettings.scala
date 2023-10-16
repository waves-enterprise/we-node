package com.wavesenterprise.settings

import cats.Show
import cats.implicits.showInterpolator
import com.wavesenterprise.utils.StringUtilites.dashes
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration
import scala.util.chaining.scalaUtilChainingOps

case class AdditionalCacheSettings(
    rocksdb: RocksDBCacheSettings,
    confidentialRocksdb: ConfidentialRocksDBCacheSettings,
    blockIds: BlockIdsCacheSettings,
    keyBlockIds: BlockIdsCacheSettings
)

object AdditionalCacheSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[AdditionalCacheSettings] = deriveReader

  implicit val toPrintable: Show[AdditionalCacheSettings] = { x =>
    import x._
    s"""
       |rocksdb:
       |  ${show"$rocksdb" pipe dashes}
       |confidentialRocksdb:
       |  ${show"$confidentialRocksdb" pipe dashes}
       |blockIds:
       |  ${show"$blockIds" pipe dashes}
       |keyBlockIds:
       |  ${show"$keyBlockIds" pipe dashes}
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

case class ConfidentialRocksDBCacheSettings(maxRollbackDepth: Int)

object ConfidentialRocksDBCacheSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[ConfidentialRocksDBCacheSettings] = deriveReader

  implicit val toPrintable: Show[ConfidentialRocksDBCacheSettings] = { x =>
    import x._
    s"""
       |maxRollbackDepth: $maxRollbackDepth
     """.stripMargin
  }
}

case class BlockIdsCacheSettings(
    maxSize: Int,
    expireAfter: FiniteDuration
)

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
