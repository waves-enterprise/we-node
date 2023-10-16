package com.wavesenterprise.network.contracts

import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.network.ConfidentialInventory
import com.wavesenterprise.network.contracts.ConfidentialDataInventoryBroadcaster.ConfidentialDataFeatureUnactivatedError
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerSession}
import com.wavesenterprise.state.{Blockchain, ContractId}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel
import monix.eval.Task

trait ConfidentialDataInventoryBroadcaster extends ScorexLogging {
  import com.wavesenterprise.features.FeatureProvider._

  protected def blockchain: Blockchain
  protected def peers: ActivePeerConnections

  def buildConfidentialInventory(
      contractId: ContractId,
      commitment: Commitment,
      dataType: ConfidentialDataType,
      signer: PrivateKeyAccount
  ): Task[ConfidentialInventory] =
    Task.defer {
      val isConfidentialDataFeatureActivated = blockchain.isFeatureActivated(BlockchainFeature.ConfidentialDataInContractsSupport, blockchain.height)

      if (isConfidentialDataFeatureActivated) {
        Task.pure(ConfidentialInventory(signer, contractId, commitment, dataType))
      } else {
        Task.raiseError(ConfidentialDataFeatureUnactivatedError(contractId, commitment, dataType))
      }
    }

  def broadcastInventory(
      inventory: ConfidentialInventory,
      flushChannels: Boolean = true,
      excludeChannels: Set[Channel] = Set.empty
  ): Unit = {
    blockchain.contract(inventory.contractId) match {
      case Some(contract) =>
        val confidentialDataRecipients = contract.groupParticipants
        broadcastInventoryToRecipients(confidentialDataRecipients, inventory, flushChannels, excludeChannels)
      case None => log.error(s"$inventory wasn't broadcast, because contract '${inventory.contractId}' not found in DB")
    }
  }

  def broadcastInventoryToRecipients(
      confidentialDataRecipients: Set[Address],
      inventory: ConfidentialInventory,
      flushChannels: Boolean = true,
      excludeChannels: Set[Channel] = Set.empty
  ): Unit = {
    peers
      .withAddresses(confidentialDataRecipients.contains, excludeWatchers = true)
      .collect {
        case PeerSession(channel, _, _) if !excludeChannels.contains(channel) => channel
      }
      .foreach(channel => if (flushChannels) channel.writeAndFlush(inventory) else channel.write(inventory))
  }
}

object ConfidentialDataInventoryBroadcaster {
  case class ConfidentialDataFeatureUnactivatedError(contractId: ContractId, commitment: Commitment, dataType: ConfidentialDataType)
      extends IllegalStateException(
        s"Confidential data feature not activated. Confidential data with contractId $contractId, commitment: ${commitment.hash}, type: $dataType"
      )

}
