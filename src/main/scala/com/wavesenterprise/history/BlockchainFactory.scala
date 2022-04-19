package com.wavesenterprise.history

import com.wavesenterprise.Schedulers
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.database.rocksdb.{RocksDBStorage, RocksDBWriter}
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.state.{BlockchainUpdaterImpl, MiningConstraintsHolder, NG}
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.utils.Time

object BlockchainFactory {

  def apply(settings: WESettings,
            storage: RocksDBStorage,
            time: Time,
            schedulers: Schedulers): (RocksDBWriter, BlockchainUpdater with PrivacyState with NG with MiningConstraintsHolder) = {
    val dbWriter = RocksDBWriter(storage, settings)
    dbWriter -> new BlockchainUpdaterImpl(dbWriter, settings, time, schedulers)
  }
}
