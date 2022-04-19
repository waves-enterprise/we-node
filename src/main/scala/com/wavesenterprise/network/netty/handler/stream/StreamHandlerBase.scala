package com.wavesenterprise.network.netty.handler.stream

import com.wavesenterprise.network.id
import com.wavesenterprise.utils.ScorexLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.{Channel, ChannelDuplexHandler, ChannelHandlerContext}
import io.netty.util.ReferenceCountUtil

import scala.concurrent.{Future, Promise}

abstract class StreamHandlerBase(channel: Channel, progressListener: StreamReadProgressListener) extends ChannelDuplexHandler with ScorexLogging {
  private[this] val startPromise      = Promise[Unit]()
  private[this] val completionPromise = Promise[Unit]()
  private[this] val errorPromise      = Promise[Unit]()

  def start: Future[Unit]      = startPromise.future
  def completion: Future[Unit] = completionPromise.future
  def error: Future[Unit]      = errorPromise.future

  protected def onNextChunk(ctx: ChannelHandlerContext, in: ByteBuf, size: Int): Unit = ctx.read()

  protected def onComplete(ctx: ChannelHandlerContext): Unit = progressListener.progressed(0, isLast = true)

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case in: ByteBuf if in.readableBytes >= Integer.BYTES && in.getInt(0) == ChunkedWriteHandler.CHUNK_MAGIC =>
      try {
        if (!start.isCompleted) {
          log.trace(s"Disable '${id(channel)}' channel auto-read")
          ctx.channel().config().setAutoRead(false)
          startPromise.success(())
        }
        in.readInt() // Magic number
        val size = in.readableBytes()
        progressListener.progressed(size)

        onNextChunk(ctx, in, size)
      } finally ReferenceCountUtil.release(in)
    case in: ByteBuf if !in.isReadable =>
      try {
        if (!completion.isCompleted) {
          onComplete(ctx)
          ctx.fireChannelReadComplete()
          completionPromise.success(())
        }
      } finally ReferenceCountUtil.release(in)
    case _ =>
      super.channelRead(ctx, msg)
  }

  /**
    * We do not pass the event to the the pipeline, because underlying handlers can call read().
    * When the last chunk is received, we will generate this event.
    */
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = ()

  /**
    * Scala does not handle Java @SuppressWarnings("deprecation") annotation.
    * This method is allowed to be used for [[io.netty.channel.ChannelInboundHandler]].
    */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    log.error(s"Stream processing error", cause)
    if (!start.isCompleted) startPromise.failure(cause)
    if (!completion.isCompleted) completionPromise.failure(cause)
    if (!errorPromise.isCompleted) errorPromise.failure(cause)
    super.exceptionCaught(ctx, cause)
  }

  def dispose(): Unit = {
    log.trace(s"Dispose '${id(channel)}' channel stream handler")
    channel.pipeline().remove(this)
    channel.config().setAutoRead(true)
    channel.read()
  }
}

object StreamHandlerBase {
  val DefaultChunkSize = 8192
}
