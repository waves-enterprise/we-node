package com.wavesenterprise.network.client

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import com.wavesenterprise.network._
import com.wavesenterprise.network.handshake._
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}

import scala.concurrent.Promise
import scala.concurrent.duration._

class ClientHandshakeHandler(handshake: SignedHandshake, promise: Promise[Channel]) extends ChannelInboundHandlerAdapter with ScorexLogging {

  private def removeHandlers(ctx: ChannelHandlerContext): Unit = {
    ctx.pipeline().remove(classOf[HandshakeTimeoutHandler])
    ctx.pipeline().remove(this)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case HandshakeTimeoutExpired =>
      log.error("Timeout expired while waiting for handshake")
      ctx.close()
      promise.failure(new IOException("No handshake"))
    case remoteHandshake: SignedHandshake =>
      if (handshake.chainId != remoteHandshake.chainId) {
        log.warn(s"Remote chain id '${remoteHandshake.chainId}' does not match local '${handshake.chainId}'")
        ctx.close()
      } else {
        remoteHandshake.nodeVersion.features.flatMap(_.triggerAttribute).foreach { featureAttribute =>
          ctx.channel().attr(featureAttribute).set(Unit)
          log.trace(s"Setting attribute '$featureAttribute' for '${id(ctx)}'")
        }
        promise.success(ctx.channel())
        log.info(s"Accepted handshake $remoteHandshake")
        removeHandlers(ctx)
      }
    case _ => super.channelRead(ctx, msg)
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    ctx.writeAndFlush(handshake.encode(ctx.alloc().buffer()))
    super.channelActive(ctx)
  }
}

// Used only in tests and Generator
class LegacyChannelInitializer(trafficLoggerSettings: TrafficLogger.Settings, handshake: SignedHandshakeV3, promise: Promise[Channel])
    extends ChannelInitializer[SocketChannel] {
  private val lengthFieldLength = 4
  private val maxFieldLength    = 1024 * 1024

  override def initChannel(ch: SocketChannel): Unit =
    ch.pipeline()
      .addLast(
        new HandshakeDecoder(),
        new HandshakeTimeoutHandler(30.seconds),
        new ClientHandshakeHandler(handshake, promise),
        new LengthFieldPrepender(lengthFieldLength),
        new LengthFieldBasedFrameDecoder(maxFieldLength, 0, lengthFieldLength, 0, lengthFieldLength),
        new MetaMessageCodec(CacheBuilder.newBuilder().expireAfterWrite(3, TimeUnit.MINUTES).build[ByteStr, Object]()),
        new TrafficLogger(trafficLoggerSettings)
      )
}
