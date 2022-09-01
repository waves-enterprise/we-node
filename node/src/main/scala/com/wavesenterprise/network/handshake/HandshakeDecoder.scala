package com.wavesenterprise.network.handshake

import com.wavesenterprise.network.{closeChannel, id}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ReplayingDecoder

import java.util
import scala.util.control.NonFatal

class HandshakeDecoder() extends ReplayingDecoder[Void] with ScorexLogging {

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    try {
      val handshake = SignedHandshake.decode(in)
      log.trace(s"'${id(ctx)}' Handshake decoded successfully: $handshake")
      out.add(handshake)
      ctx.pipeline.remove(this)
    } catch {
      case NonFatal(e) => close(ctx, e)
    }
  }

  protected def close(ctx: ChannelHandlerContext, e: Throwable): Unit = {
    log.debug(s"Failed to decode handshake from '${ctx.channel.remoteAddress}'. Reason: ${e.getMessage}")
    closeChannel(ctx.channel, e.getMessage)
  }
}
