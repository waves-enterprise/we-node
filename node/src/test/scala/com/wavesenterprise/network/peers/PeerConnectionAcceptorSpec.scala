package com.wavesenterprise.network.peers

import com.wavesenterprise.acl.{OpType, PermissionOp, Permissions, Role}
import com.wavesenterprise.network.handshake.SignedHandshakeV3
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.state.diffs.produce
import com.wavesenterprise.{ApplicationInfo, NodeVersion, TestTime, TransactionGen, Version}
import io.netty.channel.DefaultChannelId
import io.netty.channel.embedded.EmbeddedChannel
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.net.InetSocketAddress
import scala.util.Random
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PeerConnectionAcceptorSpec extends AnyFreeSpec with Matchers with MockFactory with ScalaCheckPropertyChecks with TransactionGen {

  private val time = new TestTime()

  private val applicationInstanceInfo = ApplicationInfo(
    applicationName = "waves-enterpriseI",
    nodeVersion = NodeVersion(Version.VersionTuple),
    consensusType = "pos",
    nodeName = "test",
    nodeNonce = Random.nextInt(),
    declaredAddress = None
  )

  "PeerConnectionAcceptor" - {
    "node has contract mining disabled" - {
      "accepts connections if connections count is below maximum, reject connections if maxSimultaneousConnections is reached" in {
        val maxSimultaneousConnections = 3
        val activePeerConnections      = new ActivePeerConnections(100)
        val blockchain                 = mock[Blockchain]

        val connectionAcceptor = new PeerConnectionAcceptor(activePeerConnections, maxSimultaneousConnections, blockchain, time)

        (1 to maxSimultaneousConnections).foreach { _ =>
          val connection = newConnection(connectionAcceptor, blockchain)
          connectionAcceptor.accept(connection) shouldBe 'right
        }

        activePeerConnections.connectedPeersCount shouldBe maxSimultaneousConnections

        val discardedConn = newConnection(connectionAcceptor, blockchain)
        connectionAcceptor.accept(discardedConn) should produce(
          s"Connection from '${discardedConn.remoteAddress}' reached maximum number of allowed connections ('$maxSimultaneousConnections')")
      }
    }

    "node has contract mining enabled" - {
      "accepts validator connection if connections count reached maxSimultaneousConnections" in {
        val maxSimultaneousConnections = 3
        val activePeerConnections      = new ActivePeerConnections(100)
        val blockchain                 = mock[Blockchain]

        val connectionAcceptor =
          new PeerConnectionAcceptor(activePeerConnections, maxSimultaneousConnections, blockchain, time, isContractMiningEnabled = true)

        (1 to maxSimultaneousConnections).foreach { _ =>
          val connection = newConnection(connectionAcceptor, blockchain)
          connectionAcceptor.accept(connection) shouldBe 'right
        }

        activePeerConnections.connectedPeersCount shouldBe maxSimultaneousConnections

        val validatorConn = newConnection(connectionAcceptor, blockchain, isValidator = true)
        connectionAcceptor.accept(validatorConn) shouldBe 'right

        activePeerConnections.connectedPeersCount shouldBe maxSimultaneousConnections
      }

      "reject validator connection if validators count reached maxSimultaneousConnections" in {
        val maxSimultaneousConnections = 3
        val activePeerConnections      = new ActivePeerConnections(100)
        val blockchain                 = mock[Blockchain]

        val connectionAcceptor =
          new PeerConnectionAcceptor(activePeerConnections, maxSimultaneousConnections, blockchain, time, isContractMiningEnabled = true)

        (1 to maxSimultaneousConnections).foreach { _ =>
          val connection = newConnection(connectionAcceptor, blockchain, isValidator = true)
          connectionAcceptor.accept(connection) shouldBe 'right
        }

        activePeerConnections.connectedPeersCount shouldBe maxSimultaneousConnections

        val validatorConn = newConnection(connectionAcceptor, blockchain, isValidator = true)
        connectionAcceptor.accept(validatorConn) should produce("Can't replace non validator peer – all peers are validators")

        activePeerConnections.connectedPeersCount shouldBe maxSimultaneousConnections
      }
    }
  }

  private def newConnection(connectionAcceptor: PeerConnectionAcceptor, blockchain: Blockchain, isValidator: Boolean = false): PeerConnection = {
    val sessionKey = accountGen.sample.get
    val ownerKey   = accountGen.sample.get
    val handshake  = SignedHandshakeV3.createAndSign(applicationInstanceInfo, sessionKey, ownerKey)
    val peerInfo   = PeerInfo.fromHandshake(handshake, new InetSocketAddress(12345))

    val permissions =
      if (isValidator) Permissions(Seq(PermissionOp(OpType.Add, Role.ContractValidator, time.getTimestamp(), None))) else Permissions(Seq.empty)
    (blockchain.permissions _).expects(peerInfo.nodeOwnerAddress).returning(permissions)

    connectionAcceptor.newConnection(new EmbeddedChannel(DefaultChannelId.newInstance()), peerInfo, sessionKey)
  }
}
