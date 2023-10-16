package com.wavesenterprise.network.contracts

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.database.rocksdb.confidential.{ConfidentialRocksDBStorage, PersistentConfidentialState}
import com.wavesenterprise.network.ConfidentialDataRequest
import com.wavesenterprise.network.MessageObserver.IncomingMessages
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.state.contracts.confidential.{ConfidentialDataUnit, ConfidentialStateUpdater}
import com.wavesenterprise.transaction.{BlockchainUpdater, ConfidentialContractDataUpdate}
import com.wavesenterprise.utils.{AsyncLRUCache, Time}
import monix.execution.Scheduler
import monix.reactive.Observable

class ConfidentialContractsComponents(
    val storage: ConfidentialRocksDBStorage,
    val replier: ConfidentialDataReplier,
    val inventoryHandler: ConfidentialDataInventoryHandler,
    val synchronizer: ConfidentialDataSynchronizer,
    val state: PersistentConfidentialState,
    val stateUpdater: ConfidentialStateUpdater
) extends AutoCloseable {

  // todo: add components' running to 'Application.scala' when confidential contracts feature will be ready
  def run(): Unit = {
    replier.run()
    synchronizer.run()
    inventoryHandler.run()
  }

  override def close(): Unit = {
    replier.close()
    synchronizer.close()
    inventoryHandler.close()
    storage.close()
    stateUpdater.close()
  }
}

object ConfidentialContractsComponents {

  def apply(
      settings: WESettings,
      blockchain: Blockchain with BlockchainUpdater,
      incomingMessages: IncomingMessages,
      activePeerConnections: ActivePeerConnections,
      owner: PrivateKeyAccount,
      scheduler: Scheduler,
      time: Time,
      confidentialContractDataUpdate: Observable[ConfidentialContractDataUpdate]
  ): ConfidentialContractsComponents = {

    val storage = ConfidentialRocksDBStorage.openDB(settings.confidentialContracts.dataDirectory)
    val cache   = new AsyncLRUCache[ConfidentialDataRequest, ConfidentialDataUnit](settings.confidentialContracts.cache)

    val inventoryHandler = new ConfidentialDataInventoryHandler(
      inventories = incomingMessages.confidentialInventories,
      requests = incomingMessages.confidentialInventoryRequests,
      owner = owner,
      activePeerConnections,
      blockchain,
      storage,
      settings.confidentialContracts.inventoryHandler
    )(scheduler)

    val replier = new ConfidentialDataReplier(
      blockchain,
      settings.confidentialContracts.replier,
      incomingMessages.confidentialDataRequests,
      activePeerConnections,
      storage,
      cache
    )(scheduler)

    val synchronizer = new ConfidentialDataSynchronizer(
      blockchain,
      owner,
      settings.confidentialContracts.synchronizer,
      incomingMessages.confidentialDataResponses,
      inventoryHandler,
      activePeerConnections,
      storage,
      cache,
      confidentialContractDataUpdate,
      time,
    )(scheduler)

    val state = PersistentConfidentialState(storage, settings)

    val stateUpdater = new ConfidentialStateUpdater(synchronizer, state, storage, blockchain)(scheduler)

    new ConfidentialContractsComponents(storage, replier, inventoryHandler, synchronizer, state, stateUpdater)
  }
}
