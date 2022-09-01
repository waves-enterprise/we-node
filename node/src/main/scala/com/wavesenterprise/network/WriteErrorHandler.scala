package com.wavesenterprise.network

import java.nio.channels.ClosedChannelException

import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._

@Sharable
class WriteErrorHandler extends ChannelOutboundHandlerAdapter with ScorexLogging {
  override def write(ctx: ChannelHandlerContext, msg: AnyRef, promise: ChannelPromise): Unit =
    ctx.write(
      msg,
      promise.unvoid().addListener { (chf: ChannelFuture) =>
        if (!chf.isSuccess) {
          chf.cause match {
            case _: ClosedChannelException =>
              log.debug(s"${id(ctx.channel())} Channel closed while writing (${msg.getClass.getCanonicalName})")
            case ex: java.io.IOException if ex.getMessage.contains("Connection reset") =>
              log.error(s"java.io.IOException on ${id(ctx.channel())} Write failed (${msg.getClass.getCanonicalName})")
            case _: java.io.IOException =>
              log.error(s"java.io.IOException on ${id(ctx.channel())} Write failed (${msg.getClass.getCanonicalName})", chf.cause)
            case other =>
              log.error(s"${id(ctx.channel())} Write failed (${msg.getClass.getCanonicalName})", other)
          }
        }
      }
    )
}
