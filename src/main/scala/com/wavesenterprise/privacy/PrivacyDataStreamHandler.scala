package com.wavesenterprise.privacy

import com.wavesenterprise.network.netty.handler.stream.{StreamHandlerBase, StreamReadProgressListener}
import io.netty.buffer.ByteBuf
import io.netty.channel.{Channel, ChannelHandlerContext}
import monix.catnap.ConcurrentQueue
import monix.eval.Task
import monix.execution.{BufferCapacity, ChannelType, Scheduler}
import monix.reactive.Observable

import scala.util.control.NonFatal

class PrivacyDataStreamHandler(
    channel: Channel,
    progressListener: StreamReadProgressListener,
    chunkBufferCapacity: Int
)(implicit scheduler: Scheduler)
    extends StreamHandlerBase(channel, progressListener) {

  private[this] val innerDataStream = ConcurrentQueue.unsafe[Task, Array[Byte]](BufferCapacity.Bounded(chunkBufferCapacity), ChannelType.SPMC)

  def dataStream: Observable[Array[Byte]] =
    Observable
      .repeatEvalF(innerDataStream.poll)
      .takeWhile(_.nonEmpty)
      .takeUntil(Observable.fromFuture(error))

  override def onNextChunk(ctx: ChannelHandlerContext, in: ByteBuf, size: Int): Unit = {
    val bytes = new Array[Byte](size)
    in.readBytes(bytes)
    innerDataStream
      .offer(bytes)
      .map[Unit](_ => ctx.read())
      .onErrorRecover {
        case NonFatal(ex) =>
          exceptionCaught(ctx, new RuntimeException(s"Failed to push bytes into chunks queue", ex))
      }
      .runToFuture
  }

  override protected def onComplete(ctx: ChannelHandlerContext): Unit = {
    super.onComplete(ctx)
    innerDataStream
      .offer(Array.empty)
      .onErrorRecover {
        case NonFatal(ex) =>
          exceptionCaught(ctx, new RuntimeException(s"Failed to push last part into chunks queue", ex))
      }
      .runToFuture
  }
}

object PrivacyDataStreamHandler {
  val Name = "PrivacyDataStreamHandler#0"
}
