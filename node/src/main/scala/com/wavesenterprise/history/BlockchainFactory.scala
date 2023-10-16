package com.wavesenterprise.history

import com.wavesenterprise.Schedulers
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.database.rocksdb.{MainRocksDBStorage, RocksDBWriter}
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.state.{BlockchainUpdaterImpl, MiningConstraintsHolder, NG}
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.utils.Time

object BlockchainFactory {

  def apply(settings: WESettings,
            storage: MainRocksDBStorage,
            time: Time,
            schedulers: Schedulers): (RocksDBWriter, BlockchainUpdater with PrivacyState with NG with MiningConstraintsHolder) = {
    val rocksWriter       = RocksDBWriter(storage, settings)
    val blockchainUpdater = new BlockchainUpdaterImpl(rocksWriter, settings, time, schedulers)

    rocksWriter -> blockchainUpdater
  }
}
