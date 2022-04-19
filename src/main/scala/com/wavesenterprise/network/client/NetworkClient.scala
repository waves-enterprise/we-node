package com.wavesenterprise.network.client

import java.io.IOException
import java.net.InetSocketAddress

import com.wavesenterprise.network.TrafficLogger
import com.wavesenterprise.network.handshake.SignedHandshakeV2
import com.wavesenterprise.utils.ScorexLogging
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.GlobalEventExecutor
import monix.eval.Coeval

import scala.concurrent.{Future, Promise}

class NetworkClient(trafficLoggerSettings: TrafficLogger.Settings, handshakeMaker: Coeval[SignedHandshakeV2]) extends ScorexLogging {

  private val workerGroup  = new NioEventLoopGroup()
  private val openChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

  def connect(remoteAddress: InetSocketAddress): Future[Channel] = {
    val p         = Promise[Channel]
    val handshake = handshakeMaker()

    val bootstrap = new Bootstrap()
      .group(workerGroup)
      .channel(classOf[NioSocketChannel])
      .handler(new LegacyChannelInitializer(trafficLoggerSettings, handshake, p))

    log.trace(s"Connecting to '$remoteAddress'")
    val channelFuture = bootstrap.connect(remoteAddress)
    channelFuture.addListener((_: io.netty.util.concurrent.Future[Void]) => {
      log.trace(s"Connected to '$remoteAddress'")
      channelFuture.channel().write(p)
    })

    val channel = channelFuture.channel()
    openChannels.add(channel)
    channel.closeFuture().addListener { chf: ChannelFuture =>
      if (!p.isCompleted) {
        val cause = Option(chf.cause()).getOrElse(new IllegalStateException("The connection is closed before handshake"))
        p.failure(new IOException(cause))
      }
      log.trace(s"Connection to '$remoteAddress' closed")
      openChannels.remove(chf.channel())
    }

    p.future
  }

  def shutdown(): Unit =
    try {
      openChannels.close().await()
      log.debug("Closed all channels")
    } finally {
      workerGroup.shutdownGracefully()
    }
}
