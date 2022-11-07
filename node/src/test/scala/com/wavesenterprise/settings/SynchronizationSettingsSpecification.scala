package com.wavesenterprise.settings

import com.wavesenterprise.network.InvalidBlockStorageImpl.InvalidBlockStorageSettings
import com.wavesenterprise.settings.SynchronizationSettings.{
  HistoryReplierSettings,
  KeyBlockAppendingSettings,
  MicroblockSynchronizerSettings,
  TxBroadcasterSettings,
  UtxSynchronizerSettings
}
import pureconfig.ConfigSource

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SynchronizationSettingsSpecification extends AnyFlatSpec with Matchers {

  "SynchronizationSettings" should "read values" in {
    val configSource = ConfigSource.string {
      """{
        |    max-rollback = 100
        |    max-chain-length = 101
        |    extension-batch-size = 9
        |    synchronization-timeout = 30s
        |    score-ttl = 90s
        |
        |    invalid-blocks-storage {
        |      max-size = 40000
        |      timeout = 2d
        |    }
        |
        |    history-replier {
        |      max-micro-block-cache-size = 5
        |      max-block-cache-size = 2
        |    }
        |
        |    utx-synchronizer {
        |      max-buffer-size = 777
        |      max-buffer-time = 999ms
        |    }
        |
        |    transaction-broadcaster {
        |      known-tx-cache-size = 999
        |      known-tx-cache-time = 19s
        |      min-broadcast-count = 1
        |      max-broadcast-count = 2
        |      max-batch-size = 200
        |      max-batch-time = 300ms
        |      retry-delay = 10s
        |      miners-update-interval = 10s
        |    }
        |
        |    micro-block-synchronizer {
        |      wait-response-timeout: 5s
        |      processed-micro-blocks-cache-timeout: 2s
        |      inventory-cache-timeout: 3s
        |      max-cache-size: 101
        |      crawling-parallelism: 3
        |      max-download-attempts: 2
        |    }
        |
        |    key-block-appending {
        |      max-attempts = 3
        |      retry-interval = 100ms
        |    }
        |}
      """.stripMargin
    }

    val settings = configSource.loadOrThrow[SynchronizationSettings]
    settings.maxRollback should be(100)
    settings.maxChainLength should be(101)
    settings.extensionBatchSize should be(9)
    settings.synchronizationTimeout should be(30.seconds)
    settings.scoreTtl should be(90.seconds)
    settings.invalidBlocksStorage shouldBe InvalidBlockStorageSettings(
      maxSize = 40000,
      timeout = 2.days
    )
    settings.microBlockSynchronizer shouldBe MicroblockSynchronizerSettings(
      waitResponseTimeout = 5.seconds,
      processedMicroBlocksCacheTimeout = 2.seconds,
      inventoryCacheTimeout = 3.seconds,
      maxCacheSize = 101,
      crawlingParallelism = PositiveInt(3),
      maxDownloadAttempts = PositiveInt(2)
    )
    settings.historyReplier shouldBe HistoryReplierSettings(
      maxMicroBlockCacheSize = 5,
      maxBlockCacheSize = 2
    )

    settings.transactionBroadcaster shouldBe TxBroadcasterSettings(
      knownTxCacheSize = 999,
      knownTxCacheTime = 19.seconds,
      minBroadcastCount = 1,
      maxBroadcastCount = 2,
      maxBatchSize = 200,
      maxBatchTime = 300.milliseconds,
      retryDelay = 10.seconds,
      minersUpdateInterval = 10.seconds
    )

    settings.utxSynchronizer shouldBe UtxSynchronizerSettings(777, 999.millis)

    settings.keyBlockAppending shouldBe KeyBlockAppendingSettings(PositiveInt(3), 100.millis)
  }
}
