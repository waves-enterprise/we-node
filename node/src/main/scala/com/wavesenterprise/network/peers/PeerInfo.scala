package com.wavesenterprise.network.peers

import com.wavesenterprise.NodeVersion
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.network.handshake.SignedHandshake

import java.net.{InetSocketAddress, SocketAddress}

case class PeerInfo(remoteAddress: SocketAddress,
                    declaredAddress: Option[InetSocketAddress],
                    chainId: Char,
                    nodeVersion: NodeVersion,
                    applicationConsensus: String,
                    nodeName: String,
                    nodeNonce: Long,
                    nodeOwnerAddress: Address,
                    sessionPubKey: PublicKeyAccount)

object PeerInfo {
  def fromHandshake(remoteHandshake: SignedHandshake, socketAddress: SocketAddress): PeerInfo = {
    PeerInfo(
      socketAddress,
      remoteHandshake.declaredAddress,
      remoteHandshake.chainId,
      remoteHandshake.nodeVersion,
      remoteHandshake.consensusType,
      remoteHandshake.nodeName,
      remoteHandshake.nodeNonce,
      remoteHandshake.nodeOwnerAddress,
      remoteHandshake.sessionPubKey
    )
  }
}
