package com.wavesenterprise.database.snapshot

import com.google.common.cache.CacheBuilder
import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.network.MetaMessageCodec
import com.wavesenterprise.network.handshake.SignedHandshakeV3
import com.wavesenterprise.network.netty.handler.stream.ChunkedWriteHandler
import com.wavesenterprise.network.peers.{PeerConnection, PeerInfo}
import com.wavesenterprise.{ApplicationInfo, NodeVersion, crypto}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{Channel, DefaultChannelId}
import org.scalacheck.Gen
import tools.GenHelper._

import java.net.InetSocketAddress

trait PeerConnectionGen {

  protected def randomHandshake(): Gen[(SignedHandshakeV3, PublicKeyAccount)] = {
    for {
      ownerKey  <- Gen.const(crypto.generateKeyPair()).map(PrivateKeyAccount.apply)
      handshake <- createHandshake(ownerKey)
    } yield handshake -> ownerKey
  }

  protected def createHandshake(ownerKey: PrivateKeyAccount): Gen[SignedHandshakeV3] = {
    for {
      applicationName <- Gen.alphaNumStr.retryUntil(_.nonEmpty)
      chainId         <- Gen.alphaChar
      nodeName        <- Gen.alphaNumStr.retryUntil(_.nonEmpty)
      nodeNonce       <- Gen.posNum[Int]
      appInfo = ApplicationInfo(
        applicationName = applicationName + chainId,
        nodeVersion = NodeVersion(1, 2, 3),
        consensusType = "pos",
        nodeName = nodeName,
        nodeNonce = nodeNonce,
        declaredAddress = None
      )
      sessionPublicKey <- Gen.const(crypto.generatePublicKey)
    } yield SignedHandshakeV3.createAndSign(appInfo, sessionPublicKey, ownerKey)
  }

  protected def newChannel(): EmbeddedChannel = {
    new EmbeddedChannel(DefaultChannelId.newInstance(), new ChunkedWriteHandler(), new MetaMessageCodec(CacheBuilder.newBuilder().build()))
  }

  protected def createPeerConnection(channel: Channel, ownerKey: PrivateKeyAccount): PeerConnection = {
    val handshake = createHandshake(ownerKey).generateSample()
    val peerInfo  = PeerInfo.fromHandshake(handshake, new InetSocketAddress(12345))
    new PeerConnection(channel, peerInfo, PrivateKeyAccount(crypto.generateKeyPair()))
  }

  protected def createPeerConnection(channel: Channel): (PeerConnection, PublicKeyAccount) = {
    val (handshake, ownerKey) = randomHandshake().generateSample()
    val peerInfo              = PeerInfo.fromHandshake(handshake, new InetSocketAddress(12345))
    val connection            = new PeerConnection(channel, peerInfo, PrivateKeyAccount(crypto.generateKeyPair()))
    connection -> ownerKey
  }
}
