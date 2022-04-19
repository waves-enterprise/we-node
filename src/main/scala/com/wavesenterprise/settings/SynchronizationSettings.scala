package com.wavesenterprise.settings

import cats.Show
import cats.syntax.show._
import com.wavesenterprise.utils.StringUtils.dashes
import com.wavesenterprise.utils.chaining._
import com.wavesenterprise.network.InvalidBlockStorageImpl.InvalidBlockStorageSettings
import com.wavesenterprise.settings.SynchronizationSettings._
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

case class SynchronizationSettings(maxRollback: Int,
                                   maxChainLength: Int,
                                   extensionBatchSize: Int,
                                   synchronizationTimeout: FiniteDuration,
                                   scoreTtl: FiniteDuration,
                                   invalidBlocksStorage: InvalidBlockStorageSettings,
                                   microBlockSynchronizer: MicroblockSynchronizerSettings,
                                   historyReplier: HistoryReplierSettings,
                                   utxSynchronizer: UtxSynchronizerSettings,
                                   transactionBroadcaster: TxBroadcasterSettings,
                                   keyBlockAppending: KeyBlockAppendingSettings)

object SynchronizationSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[SynchronizationSettings] = deriveReader

  implicit val toPrintable: Show[SynchronizationSettings] = { x =>
    import x._

    s"""
       |maxRollback: $maxRollback
       |maxChainLength: $maxChainLength
       |extensionBatchSize: $extensionBatchSize
       |synchronizationTimeout: $synchronizationTimeout
       |scoreTTL: $scoreTtl
       |invalidBlocksStorage: ${invalidBlocksStorage.show pipe dashes}
       |microBlockSynchronizer: ${microBlockSynchronizer.show pipe dashes}
       |historyReplierSettings: ${historyReplier.show pipe dashes}
       |utxSynchronizerSettings: ${utxSynchronizer.show pipe dashes}
       |transactionBroadcasterSettings: ${transactionBroadcaster.show pipe dashes}
       |keyBlockAppendingSettings: ${keyBlockAppending.show pipe dashes}
     """.stripMargin
  }

  case class MicroblockSynchronizerSettings(waitResponseTimeout: FiniteDuration,
                                            processedMicroBlocksCacheTimeout: FiniteDuration,
                                            inventoryCacheTimeout: FiniteDuration,
                                            maxCacheSize: Int,
                                            maxDownloadAttempts: PositiveInt,
                                            crawlingParallelism: PositiveInt)

  object MicroblockSynchronizerSettings extends WEConfigReaders {

    implicit val configReader: ConfigReader[MicroblockSynchronizerSettings] = deriveReader

    implicit val toPrintable: Show[MicroblockSynchronizerSettings] = { x =>
      import x._
      s"""
         |waitResponseTimeout: $waitResponseTimeout
         |processedMicroBlocksCacheTimeout: $processedMicroBlocksCacheTimeout
         |inventoryCacheTimeout: $inventoryCacheTimeout
         |maxCacheSize: $maxCacheSize
         |maxDownloadAttempts: ${maxDownloadAttempts.value}
         |crawlingParallelism: ${crawlingParallelism.value}
       """.stripMargin
    }
  }

  case class HistoryReplierSettings(maxMicroBlockCacheSize: Int, maxBlockCacheSize: Int)

  object HistoryReplierSettings extends WEConfigReaders {

    implicit val configReader: ConfigReader[HistoryReplierSettings] = deriveReader

    implicit val toPrintable: Show[HistoryReplierSettings] = { x =>
      import x._
      s"""
         |maxMicroBlockCacheSize: $maxMicroBlockCacheSize
         |maxBlockCacheSize: $maxBlockCacheSize
       """.stripMargin
    }
  }

  case class UtxSynchronizerSettings(maxBufferSize: Int, maxBufferTime: FiniteDuration)

  object UtxSynchronizerSettings extends WEConfigReaders {

    implicit val configReader: ConfigReader[UtxSynchronizerSettings] = deriveReader
    implicit val toPrintable: Show[UtxSynchronizerSettings] = { x =>
      import x._
      s"""
         |maxBufferSize: $maxBufferSize
         |maxBufferTime: $maxBufferTime
       """.stripMargin
    }
  }

  case class TxBroadcasterSettings(knownTxCacheSize: Int,
                                   knownTxCacheTime: FiniteDuration,
                                   minBroadcastCount: Int,
                                   maxBroadcastCount: Int,
                                   maxBatchSize: Int,
                                   maxBatchTime: FiniteDuration,
                                   retryDelay: FiniteDuration) {
    require(
      minBroadcastCount <= maxBroadcastCount,
      s"Configuration error: max-broadcast-count '$maxBroadcastCount' cannot be less than min-broadcast-count $minBroadcastCount"
    )
  }
  object TxBroadcasterSettings {
    implicit val configReader: ConfigReader[TxBroadcasterSettings] = deriveReader

    implicit val toPrintable: Show[TxBroadcasterSettings] = { x =>
      import x._
      s"""
         |knownTxCacheSize: $knownTxCacheSize
         |knownTxCacheTime: $knownTxCacheTime
         |minBroadcastCount: $minBroadcastCount
         |maxBroadcastCount: $maxBroadcastCount
         |maxBatchSize: $maxBatchSize
         |maxBatchTime: $maxBatchTime
         |retryDelay: $retryDelay
       """.stripMargin
    }
  }

  case class KeyBlockAppendingSettings(maxAttempts: PositiveInt, retryInterval: FiniteDuration)
  object KeyBlockAppendingSettings {
    implicit val configReader: ConfigReader[KeyBlockAppendingSettings] = deriveReader

    implicit val toPrintable: Show[KeyBlockAppendingSettings] = { x =>
      import x._
      s"""
         |maxAttempts: ${maxAttempts.value}
         |retryInterval: $retryInterval
       """.stripMargin
    }
  }
}
