package com.wavesenterprise.network

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.network.handshake._
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerConnection, PeerConnectionAcceptor, PeerInfo}
import com.wavesenterprise.privacy.InitialParticipantsDiscoverResult
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.Time
import com.wavesenterprise.{ApplicationInfo, NodeVersion, TransactionGen, Version, crypto}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import org.scalamock.scalatest.MockFactory
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Random

class ClientSpec extends AnyFreeSpec with Matchers with MockFactory with TransactionGen {

  private val clientApplicationInstanceInfo = ApplicationInfo(
    applicationName = "waves-enterpriseI",
    nodeVersion = NodeVersion(Version.VersionTuple),
    consensusType = "pos",
    nodeName = "test",
    nodeNonce = Random.nextInt(),
    declaredAddress = None
  )

  "should send only a local handshake on connection" in {
    val activePeerConnections = new ActivePeerConnections()
    val channel               = createEmbeddedChannel(activePeerConnections, clientApplicationInstanceInfo)

    val sentClientHandshakeBuff = channel.readOutbound[ByteBuf]()
    val decodedHandshake        = SignedHandshake.decode(sentClientHandshakeBuff)
    decodedHandshake.chainId shouldBe clientApplicationInstanceInfo.chainId
    decodedHandshake.nodeVersion shouldBe clientApplicationInstanceInfo.nodeVersion
    decodedHandshake.consensusType shouldBe clientApplicationInstanceInfo.consensusType
    decodedHandshake.nodeName shouldBe clientApplicationInstanceInfo.nodeName
    decodedHandshake.nodeNonce shouldBe clientApplicationInstanceInfo.nodeNonce
    decodedHandshake.declaredAddress shouldBe clientApplicationInstanceInfo.declaredAddress
    channel.outboundMessages() shouldBe empty
  }

  "should add a server's channel to all channels after the handshake only" in {
    val activePeerConnections       = new ActivePeerConnectionsForTests()
    val sessionPubKey               = crypto.generatePublicKey
    val ownerKey: PrivateKeyAccount = accountGen.sample.get
    val handshake                   = SignedHandshakeV3.createAndSign(clientApplicationInstanceInfo, sessionPubKey, ownerKey)

    val otherOwnerKey = PrivateKeyAccount(crypto.generateSessionKeyPair())
    val blockchain    = mock[Blockchain]
    (blockchain.participantPubKey _).expects(handshake.nodeOwnerAddress).returning(Some(ownerKey)).once()
    val channel = createEmbeddedChannel(activePeerConnections, clientApplicationInstanceInfo, blockchain, ownerKey = otherOwnerKey)

    // skip the client's handshake
    channel.readOutbound[ByteBuf]()
    activePeerConnections.inChannelGroup(channel) shouldBe false

    val replyServerHandshakeBuff = Unpooled.buffer()

    handshake.encode(replyServerHandshakeBuff)

    channel.writeInbound(replyServerHandshakeBuff)
    activePeerConnections.inChannelGroup(channel) shouldBe true
  }

  "shouldn't connect to self" in {
    val sameOwnerKey       = PrivateKeyAccount(crypto.generateSessionKeyPair())
    val connectionAcceptor = new PeerConnectionAcceptorForTest(new ActivePeerConnections())
    val channel = new EmbeddedChannel(
      new HandshakeDecoder(),
      new HandshakeTimeoutHandler(1.minute),
      new HandshakeHandler.Server(
        blockchain = mock[Blockchain],
        applicationInstanceInfo = clientApplicationInstanceInfo,
        ownerKey = sameOwnerKey,
        connectPromise = Promise[PeerConnection],
        initialParticipantsDiscoverResult = InitialParticipantsDiscoverResult.NotNeeded,
        connectionAcceptor = connectionAcceptor
      )
    )

    val buff      = Unpooled.buffer
    val handshake = SignedHandshakeV3.createAndSign(clientApplicationInstanceInfo, crypto.generatePublicKey, sameOwnerKey)
    handshake.encode(buff)
    buff.writeCharSequence("foo", StandardCharsets.UTF_8)

    channel.writeInbound(buff)
    channel.isOpen shouldBe false
  }

  "should not accept handshake with other consensus type" in {
    val ownerKey                            = accountGen.sample.get
    val poaConsensusApplicationInstanceInfo = clientApplicationInstanceInfo.copy(consensusType = "poa")

    val blockchain         = mock[Blockchain]
    val clientOwnerKey     = PrivateKeyAccount(crypto.generateSessionKeyPair())
    val connectionAcceptor = new PeerConnectionAcceptorForTest(new ActivePeerConnections())
    val channel = new EmbeddedChannel(
      new HandshakeDecoder(),
      new HandshakeTimeoutHandler(1.minute),
      new HandshakeHandler.Server(
        blockchain = blockchain,
        applicationInstanceInfo = poaConsensusApplicationInstanceInfo,
        ownerKey = clientOwnerKey,
        connectPromise = Promise[PeerConnection],
        initialParticipantsDiscoverResult = InitialParticipantsDiscoverResult.NotNeeded,
        connectionAcceptor = connectionAcceptor
      )
    )

    val buff      = Unpooled.buffer
    val handshake = SignedHandshakeV3.createAndSign(clientApplicationInstanceInfo, crypto.generatePublicKey, ownerKey)
    handshake.encode(buff)
    buff.writeCharSequence("foo", StandardCharsets.UTF_8)

    channel.writeInbound(buff)
    channel.isOpen shouldBe false
  }

  "should accept signedHandshake with accepted address if privacy settings expect signedHandshake" in {
    val blockchain = mock[Blockchain]

    val ownerAccount    = accountGen.sample.get
    val address         = ownerAccount.toAddress
    val sessionKey      = crypto.generatePublicKey
    val signedHandshake = SignedHandshakeV3.createAndSign(clientApplicationInstanceInfo, sessionKey, ownerAccount)

    (blockchain.participantPubKey _).expects(address).returning(Some(ownerAccount)).once()

    val activePeerConnections = new ActivePeerConnectionsForTests()

    val channel = createEmbeddedChannel(activePeerConnections, clientApplicationInstanceInfo, blockchain)
    channel.readOutbound[ByteBuf]()
    activePeerConnections.inChannelGroup(channel) shouldBe false

    val replyServerHandshakeBuff = Unpooled.buffer()
    signedHandshake.encode(replyServerHandshakeBuff)
    channel.writeInbound(replyServerHandshakeBuff)
    activePeerConnections.inChannelGroup(channel) shouldBe true
  }

  "should not accept signedHandshake with not accepted address if privacy settings expect signedHandshake" in {
    val blockchain = mock[Blockchain]

    val senderAccount      = accountGen.sample.get
    val serverOwnerAccount = accountGen.sample.get
    val address            = senderAccount.toAddress
    val sessionKey         = crypto.generatePublicKey
    val signedHandshake    = SignedHandshakeV3.createAndSign(clientApplicationInstanceInfo, sessionKey, senderAccount)

    (blockchain.participantPubKey _).expects(address).returning(None).once()
    val connectionAcceptor = new PeerConnectionAcceptorForTest(new ActivePeerConnections())
    val channel = new EmbeddedChannel(
      new HandshakeDecoder(),
      new HandshakeTimeoutHandler(1.minute),
      new HandshakeHandler.Server(
        blockchain = blockchain,
        applicationInstanceInfo = clientApplicationInstanceInfo,
        ownerKey = serverOwnerAccount,
        connectPromise = Promise[PeerConnection],
        initialParticipantsDiscoverResult = InitialParticipantsDiscoverResult.NotNeeded,
        connectionAcceptor = connectionAcceptor
      )
    )
    channel.readOutbound[ByteBuf]()

    val replyServerHandshakeBuff = Unpooled.buffer()
    signedHandshake.encode(replyServerHandshakeBuff)
    channel.writeInbound(replyServerHandshakeBuff)
    channel.isOpen shouldBe false
  }

  "should not accept signedHandshake with invalid signature if privacy settings expect signedHandshake" in {
    val blockchain = mock[Blockchain]

    val account            = accountGen.sample.get
    val serverOwnerAccount = accountGen.sample.get
    val address            = account.toAddress
    val sessionKey         = crypto.generatePublicKey
    val signature          = crypto.sign(account, "some string".getBytes(StandardCharsets.UTF_8))
    val payload = HandshakeV3Payload(
      clientApplicationInstanceInfo.chainId,
      clientApplicationInstanceInfo.nodeVersion,
      clientApplicationInstanceInfo.consensusType,
      clientApplicationInstanceInfo.declaredAddress,
      clientApplicationInstanceInfo.nodeNonce,
      clientApplicationInstanceInfo.nodeName,
      sessionKey
    )
    val signedHandshake = SignedHandshakeV3(payload, payload.bytes(), address, signature)

    (blockchain.participantPubKey _).expects(address).returning(Some(account)).once()

    val connectionAcceptor = new PeerConnectionAcceptorForTest(new ActivePeerConnections())
    val channel = new EmbeddedChannel(
      new HandshakeDecoder(),
      new HandshakeTimeoutHandler(1.minute),
      new HandshakeHandler.Server(
        blockchain = blockchain,
        applicationInstanceInfo = clientApplicationInstanceInfo,
        ownerKey = serverOwnerAccount,
        connectPromise = Promise[PeerConnection],
        initialParticipantsDiscoverResult = InitialParticipantsDiscoverResult.NotNeeded,
        connectionAcceptor = connectionAcceptor
      )
    )
    channel.readOutbound[ByteBuf]()

    val replyServerHandshakeBuff = Unpooled.buffer()
    signedHandshake.encode(replyServerHandshakeBuff)
    channel.writeInbound(replyServerHandshakeBuff)
    channel.isOpen shouldBe false
  }

  private def createEmbeddedChannel(activePeerConnections: ActivePeerConnections,
                                    applicationInstanceInfo: ApplicationInfo,
                                    blockchain: Blockchain = mock[Blockchain],
                                    ownerKey: PrivateKeyAccount = PrivateKeyAccount(crypto.generateSessionKeyPair())) = {
    val connectionAcceptor = new PeerConnectionAcceptorForTest(activePeerConnections)
    new EmbeddedChannel(
      new HandshakeDecoder(),
      new HandshakeTimeoutHandler(1.minute),
      new HandshakeHandler.Client(
        blockchain = blockchain,
        applicationInstanceInfo = applicationInstanceInfo,
        ownerKey = ownerKey,
        connectPromise = Promise[PeerConnection],
        initialParticipantsDiscoverResult = InitialParticipantsDiscoverResult.NotNeeded,
        connectionAcceptor = connectionAcceptor
      )
    )
  }

  private class ActivePeerConnectionsForTests() extends ActivePeerConnections {
    def inChannelGroup(ch: Channel): Boolean = {
      connectedChannels.contains(ch)
    }
  }

  private class PeerConnectionAcceptorForTest(activePeerConnections: ActivePeerConnections,
                                              blockchain: Blockchain = mock[Blockchain],
                                              time: Time = mock[Time])
      extends PeerConnectionAcceptor(activePeerConnections, 2, blockchain, time) {
    override def newConnection(channel: Channel, peerInfo: PeerInfo, sessionKey: PrivateKeyAccount): PeerConnection = {
      new PeerConnection(channel, peerInfo, sessionKey)
    }
  }
}
