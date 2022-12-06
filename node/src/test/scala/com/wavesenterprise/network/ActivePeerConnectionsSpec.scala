package com.wavesenterprise.network

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.network.handshake.SignedHandshakeV3
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerConnection, PeerInfo}
import com.wavesenterprise.{ApplicationInfo, NodeVersion, crypto}
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.local.LocalChannel
import org.scalacheck._
import org.scalamock.scalatest.MockFactory
import tools.GenHelper._

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.util.Random
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ActivePeerConnectionsSpec extends AnyFreeSpec with Matchers with MockFactory {

  val randomHandshake: Gen[SignedHandshakeV3] = {
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
      ownerKey         <- Gen.const(crypto.generateKeyPair()).map(PrivateKeyAccount.apply)
    } yield SignedHandshakeV3.createAndSign(appInfo, sessionPublicKey, ownerKey)
  }

  "Take a single object through a mutation journey" - {
    val activeConnections = new ActivePeerConnections(100)
    val dummyChannel      = new LocalChannel()
    val handshake         = randomHandshake.generateSample()
    val peerInfo          = PeerInfo.fromHandshake(handshake, new InetSocketAddress(12345))
    val peerConnection    = new PeerConnection(dummyChannel, peerInfo, PrivateKeyAccount(crypto.generateKeyPair()))

    "isEmpty on new object" in {
      activeConnections.isEmpty shouldBe true
    }
    "adding a connection, checking isEmpty and connectedPeersCount" in {
      activeConnections.putIfAbsentAndMaxNotReachedOrReplaceValidator(peerConnection)

      activeConnections.isEmpty shouldBe false
      withClue("ActivePeersConnections.connectedChannels should also be non-empty after putOrUpdate") {
        activeConnections.connectedPeersCount() shouldBe 1
      }
    }
    "check other reading methods" in {
      val peerAddress = activeConnections.addressForChannel(dummyChannel)
      peerAddress shouldBe 'defined

      val channelFromConnections = activeConnections.channelForAddress(peerAddress.get)
      channelFromConnections shouldBe 'defined
      channelFromConnections.get shouldBe dummyChannel

      val peerConnectionOpt = activeConnections.peerConnection(dummyChannel)
      peerConnectionOpt shouldBe 'defined
      peerConnectionOpt.get shouldBe peerConnection
    }
    "cannot put a connection, if there is one" in {
      val peerConnectionAnother = new PeerConnection(dummyChannel, peerInfo, PrivateKeyAccount(crypto.generateKeyPair()))
      activeConnections.putIfAbsentAndMaxNotReachedOrReplaceValidator(peerConnectionAnother) shouldBe 'left
    }
    "deleting a connection, should be empty again" in {
      activeConnections.remove(peerConnection)
      activeConnections.isEmpty shouldBe true
      activeConnections.connectedPeersCount() shouldBe 0
    }
  }

  "broadcast" - {
    "should not send a message to the excluded channels" in {
      val message = "test"

      val activePeerConnections = new ActivePeerConnections(100)
      val received              = ConcurrentHashMap.newKeySet[Int]()

      def receiver(id: Int): Channel = new EmbeddedChannel(
        new ChannelId {
          override def asShortText(): String        = asLongText()
          override def asLongText(): String         = id.toString
          override def compareTo(o: ChannelId): Int = o.asLongText().toInt - id
        },
        new ChannelOutboundHandlerAdapter {
          override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
            received.add(id)
            super.write(ctx, msg, promise)
          }
        }
      )

      val allIds      = (0 to 5).toSet
      val allChannels = allIds.map(receiver)

      val excludedChannels = allChannels.filter(_ => Random.nextBoolean)
      val excludedIds      = excludedChannels.map(_.id.asLongText().toInt)

      allChannels.foreach { ch =>
        val handshake      = randomHandshake.generateSample()
        val peerInfo       = PeerInfo.fromHandshake(handshake, ch.remoteAddress)
        val peerConnection = new PeerConnection(ch, peerInfo, PrivateKeyAccount(crypto.generateKeyPair()))
        activePeerConnections.putIfAbsentAndMaxNotReachedOrReplaceValidator(peerConnection)
      }
      activePeerConnections.broadcast(message, excludedChannels).syncUninterruptibly()

      received.asScala shouldBe (allIds -- excludedIds)
    }
  }
}
