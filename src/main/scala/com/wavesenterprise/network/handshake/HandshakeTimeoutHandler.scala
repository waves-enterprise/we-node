package com.wavesenterprise.network.handshake

import java.util.concurrent.TimeUnit

import com.wavesenterprise.network.id
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.util.concurrent.ScheduledFuture

import scala.concurrent.duration.FiniteDuration

case object HandshakeTimeoutExpired

class HandshakeTimeoutHandler(handshakeTimeout: FiniteDuration) extends ChannelInboundHandlerAdapter with ScorexLogging {
  private var timeout: Option[ScheduledFuture[_]] = None

  private def cancelTimeout(): Unit = timeout.foreach(_.cancel(true))

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    log.trace(s"'${id(ctx)}' Scheduling handshake timeout, timeout = $handshakeTimeout")
    timeout = Some(
      ctx
        .channel()
        .eventLoop()
        .schedule(
          { () =>
            log.trace(s"'${id(ctx)}' Firing handshake timeout expired")
            ctx.fireChannelRead(HandshakeTimeoutExpired)
          },
          handshakeTimeout.toMillis,
          TimeUnit.MILLISECONDS
        ))

    super.channelActive(ctx)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    cancelTimeout()
    super.channelInactive(ctx)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case hs: SignedHandshake =>
      cancelTimeout()
      super.channelRead(ctx, hs)
    case other =>
      super.channelRead(ctx, other)
  }
}
