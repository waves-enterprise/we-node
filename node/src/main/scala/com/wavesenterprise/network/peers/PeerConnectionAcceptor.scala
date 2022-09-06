package com.wavesenterprise.network.peers

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.acl.Role
import com.wavesenterprise.network.Attributes._
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.Time
import io.netty.channel.Channel

/**
  * Creates and accepts connections from peers.
  * Controls connections not to exceed [[maxSimultaneousConnections]] param.
  */
class PeerConnectionAcceptor(activePeerConnections: ActivePeerConnections,
                             maxSimultaneousConnections: Int,
                             blockchain: Blockchain,
                             time: Time,
                             isContractMiningEnabled: Boolean = false)
    extends AutoCloseable {

  def newConnection(channel: Channel, peerInfo: PeerInfo, sessionKey: PrivateKeyAccount): PeerConnection = {
    val timeStamp   = time.getTimestamp()
    val permissions = blockchain.permissions(peerInfo.nodeOwnerAddress)
    val isValidator = permissions.contains(Role.ContractValidator, timeStamp)
    val isMiner     = permissions.contains(Role.Miner, timeStamp)
    if (isMiner) channel.setAttrWithLogging(MinerAttribute, ())
    if (isValidator) channel.setAttrWithLogging(ValidatorAttribute, ())
    new PeerConnection(channel, peerInfo, sessionKey)
  }

  def accept(peerConnection: PeerConnection): Either[String, PeerConnection] = {
    if (activePeerConnections.connectedPeersCount() < maxSimultaneousConnections) {
      activePeerConnections.putIfAbsent(peerConnection)
    } else if (peerConnection.isValidator && isContractMiningEnabled) {
      activePeerConnections.replaceNonValidator(peerConnection)
    } else {
      Left(s"Connection from '${peerConnection.remoteAddress}' reached maximum number of allowed connections ('$maxSimultaneousConnections')")
    }
  }

  override def close(): Unit = {
    activePeerConnections.close()
  }
}
