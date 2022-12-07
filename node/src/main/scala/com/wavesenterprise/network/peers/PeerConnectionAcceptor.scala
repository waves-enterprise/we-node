package com.wavesenterprise.network.peers

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.Time
import io.netty.channel.Channel

/**
  * Creates and accepts connections from peers.
  */
class PeerConnectionAcceptor(activePeerConnections: ActivePeerConnections,
                             maxSimultaneousConnections: Int,
                             blockchain: Blockchain,
                             time: Time,
                             isContractMiningEnabled: Boolean = false)
    extends AutoCloseable
    with MinerOrValidatorAttrSet {

  def newConnection(channel: Channel, peerInfo: PeerInfo, sessionKey: PrivateKeyAccount): PeerConnection = {
    val timeStamp   = time.getTimestamp()
    val permissions = blockchain.permissions(peerInfo.nodeOwnerAddress)
    setMinerOrValidatorAttr(channel, permissions, timeStamp)
    new PeerConnection(channel, peerInfo, sessionKey)
  }

  def accept(peerConnection: PeerConnection): Either[String, PeerConnection] = {
    if (activePeerConnections.connectedPeersCount() < maxSimultaneousConnections) {
      activePeerConnections.putIfAbsentAndMaxNotReachedOrReplaceValidator(peerConnection)
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
