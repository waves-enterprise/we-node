package com.wavesenterprise.privacy

import akka.actor.ActorSystem
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.network.MessageObserver.IncomingMessages
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.privacy._
import com.wavesenterprise.privacy.db.PolicyPostgresStorageService
import com.wavesenterprise.privacy.s3.PolicyS3StorageService
import com.wavesenterprise.settings.{NodeMode, WESettings}
import com.wavesenterprise.state.NG
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.utils.Time
import monix.execution.Scheduler

class PrivacyComponents(
    val storage: PolicyStorage,
    val replier: PolicyDataReplier,
    val synchronizer: PolicyDataSynchronizer,
    val maybeInventoryHandler: Option[PrivacyInventoryHandler],
    val microBlockHandler: PrivacyMicroBlockHandler
) extends AutoCloseable {

  def run(): Unit = {
    replier.run()
    synchronizer.run()
    maybeInventoryHandler.foreach(_.run())
  }

  override def close(): Unit = {
    storage.close()
    replier.close()
    synchronizer.close()
    maybeInventoryHandler.foreach(_.close())
  }
}

object PrivacyComponents {

  def apply(
      settings: WESettings,
      state: BlockchainUpdater with PrivacyState with NG,
      incomingMessages: IncomingMessages,
      activePeerConnections: ActivePeerConnections,
      scheduler: Scheduler,
      time: Time,
      owner: PrivateKeyAccount,
      shutdownFunction: () => Unit
  )(implicit actorSystem: ActorSystem): PrivacyComponents = {
    val policyStorage = PolicyStorage.create(settings, time, shutdownFunction)(actorSystem)

    val (policyDataReplier, policyDataSynchronizer, maybeInventoryHandler, microBlockHandler) = policyStorage match {
      case _: PolicyPostgresStorageService | _: PolicyS3StorageService if settings.network.mode == NodeMode.Default =>
        val policyCache = new PolicyStrictDataCache(settings.privacy.cache)

        val inventoryHandler = new PrivacyInventoryHandler(
          inventories = incomingMessages.privacyInventories,
          requests = incomingMessages.privacyInventoryRequests,
          owner = owner,
          peers = activePeerConnections,
          state = state,
          storage = policyStorage,
          settings = settings.privacy.inventoryHandler
        )(scheduler)

        val replier = new EnablePolicyDataReplier(
          state = state,
          settings = settings.privacy.replier,
          requests = incomingMessages.privateDataRequests,
          peers = activePeerConnections,
          storage = policyStorage,
          strictDataCache = policyCache
        )(scheduler, actorSystem)

        val synchronizer = new EnablePolicyDataSynchronizer(
          state = state,
          owner = owner,
          settings = settings.privacy.synchronizer,
          responses = incomingMessages.privateDataResponses,
          privacyInventoryHandler = inventoryHandler,
          peers = activePeerConnections,
          maxSimultaneousConnections = settings.network.maxSimultaneousConnections,
          storage = policyStorage,
          strictDataCache = policyCache,
          time = time
        )(scheduler)

        val microBlockHandler = new EnabledPrivacyMicroBlockHandler(
          owner = owner,
          peers = activePeerConnections,
          state = state,
          storage = policyStorage
        )

        (replier, synchronizer, Some(inventoryHandler), microBlockHandler)
      case _ =>
        (NoOpPolicyDataReplier, NoOpPolicyDataSynchronizer, None, NoOpPrivacyMicroBlockHandler)
    }

    new PrivacyComponents(
      policyStorage,
      policyDataReplier,
      policyDataSynchronizer,
      maybeInventoryHandler,
      microBlockHandler
    )
  }
}
