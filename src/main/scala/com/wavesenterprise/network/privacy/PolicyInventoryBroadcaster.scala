package com.wavesenterprise.network.privacy

import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.network.Attributes.PrivacyProtocolExtensionV1Attribute
import com.wavesenterprise.network.{PrivacyInventory, PrivacyInventoryV1, PrivacyInventoryV2}
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerSession}
import com.wavesenterprise.network.privacy.PolicyInventoryBroadcaster.InventoryV2FeatureUnactivatedError
import com.wavesenterprise.privacy.{PolicyDataHash, PrivacyDataType}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import io.netty.channel.Channel
import monix.eval.Task

trait PolicyInventoryBroadcaster {
  import com.wavesenterprise.features.FeatureProvider._

  protected def state: Blockchain
  protected def peers: ActivePeerConnections

  def buildPrivacyInventory(
      dataType: PrivacyDataType,
      policyId: ByteStr,
      dataHash: PolicyDataHash,
      signer: PrivateKeyAccount
  ): Task[PrivacyInventory] =
    Task.defer {
      val inventoryV2FeatureActivated = state.isFeatureActivated(BlockchainFeature.PrivacyLargeObjectSupport, state.height)

      (dataType, inventoryV2FeatureActivated) match {
        case (PrivacyDataType.Large, false)   => Task.raiseError(InventoryV2FeatureUnactivatedError(policyId, dataHash))
        case (PrivacyDataType.Default, false) => Task.pure(PrivacyInventoryV1(signer, policyId, dataHash))
        case (_, true)                        => Task.pure(PrivacyInventoryV2(signer, policyId, dataHash, dataType))
      }
    }

  def broadcastInventory(
      inventory: PrivacyInventory,
      flushChannels: Boolean = true,
      excludeChannels: Set[Channel] = Set.empty
  ): Unit = {
    val policyRecipients = state.policyRecipients(inventory.policyId)
    broadcastInventoryToRecipients(policyRecipients, inventory, flushChannels, excludeChannels)
  }

  def broadcastInventoryToRecipients(
      policyRecipients: Set[Address],
      inventory: PrivacyInventory,
      flushChannels: Boolean = true,
      excludeChannels: Set[Channel] = Set.empty
  ): Unit = {
    peers
      .withAddresses(policyRecipients.contains, excludeWatchers = true)
      .collect {
        case PeerSession(channel, _, _) if !excludeChannels.contains(channel) && channel.hasAttr(PrivacyProtocolExtensionV1Attribute) => channel
      }
      .foreach(channel => if (flushChannels) channel.writeAndFlush(inventory) else channel.write(inventory))
  }
}

object PolicyInventoryBroadcaster {
  case class InventoryV2FeatureUnactivatedError(policyId: ByteStr, dataHash: PolicyDataHash)
      extends IllegalStateException(s"Support of large object feature not activated. Policy '$policyId' data '$dataHash' cannot be processed.")
}
