package com.wavesenterprise.network.peers

import com.wavesenterprise.network.handshake.SignedHandshake
import com.wavesenterprise.network.{ChannelHandlerContextExt, EventExecutorGroupExt, GetPeersV2, KnownPeersV2, PeerHostname, id}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

import java.net.InetSocketAddress
import scala.concurrent.duration.FiniteDuration

class PeerSynchronizer(peerDatabase: PeerDatabase, peerRequestInterval: FiniteDuration) extends ChannelInboundHandlerAdapter with ScorexLogging {

  private var declaredAddress = Option.empty[InetSocketAddress]

  def requestPeers(ctx: ChannelHandlerContext): Unit = if (ctx.channel.isActive) {
    ctx.writeAndFlush(GetPeersV2)

    ctx.executor.schedule(peerRequestInterval) {
      requestPeers(ctx)
    }
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
    declaredAddress.foreach(peerDatabase.touch)
    msg match {
      case handshake: SignedHandshake =>
        val remoteAddressOpt = compareDeclaredWithRealAddress(ctx, handshake)

        remoteAddressOpt match {
          case Some(remoteAddress) =>
            log.trace(s"'${id(ctx)}' Touching remote address '$remoteAddress'")
            peerDatabase.touch(remoteAddress)
            declaredAddress = Some(remoteAddress)

          case None => ()
        }

        requestPeers(ctx)
        super.channelRead(ctx, msg)

      case KnownPeersV2(peerHostnames) =>
        val peers = peerHostnames.map(peer => new InetSocketAddress(peer.hostname, peer.port))
        // we use Address everywhere, so to avoid NPE filter it here
        val (goodPeers, unresolvedPeers) = peers.partition(_.getAddress != null)
        log.trace(s"receive peers which addresses can't be obtained: [${unresolvedPeers.mkString(", ")}]")

        val added    = peerDatabase.addCandidates(goodPeers)
        val notAdded = goodPeers.filter(added.contains)
        log.trace(s"'${id(ctx)}' Added peers: '${format(added)}', not added peers: '${format(notAdded)}'")

      case GetPeersV2 =>
        val knownPeers = peerDatabase.knownPeers.keys
        val peerHostnames = knownPeers
          .flatMap(isa =>
            Option(isa.getHostName) match {
              case Some(hostname) => Some(PeerHostname(hostname, isa.getPort))
              case None           => None
            })
          .toSeq
        ctx.writeAndFlush(KnownPeersV2(peerHostnames))

      case _ =>
        super.channelRead(ctx, msg)
    }
  }

  private def compareDeclaredWithRealAddress(ctx: ChannelHandlerContext, handshake: SignedHandshake): Option[InetSocketAddress] = {
    val remoteSocketAddressOpt = ctx.remoteAddressOpt
    for {
      fromHandshake        <- handshake.declaredAddress
      addressFromHandshake <- Option(fromHandshake.getAddress)
      ctxAddress           <- remoteSocketAddressOpt.map(_.getAddress)
      if addressFromHandshake == ctxAddress
    } yield fromHandshake
  }

  private def format[T](xs: Iterable[T]): String = xs.mkString("[", ", ", "]")
}

object PeerSynchronizer {

  @Sharable
  class NoopPeerSynchronizer extends ChannelInboundHandlerAdapter {

    override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
      super.channelRead(ctx, msg)
    }
  }

  val Disabled = new NoopPeerSynchronizer()

}
