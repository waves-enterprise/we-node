package com.wavesenterprise.database.snapshot

import com.wavesenterprise.network.netty.handler.stream.{StreamHandlerBase, StreamReadProgressListener}
import io.netty.buffer.ByteBuf
import io.netty.channel.{Channel, ChannelHandlerContext}

import java.nio.channels.FileChannel

class SnapshotDataStreamHandler(channel: Channel, fileChannel: FileChannel, progressListener: StreamReadProgressListener)
    extends StreamHandlerBase(channel, progressListener) {

  override protected def onNextChunk(ctx: ChannelHandlerContext, in: ByteBuf, size: Int): Unit = {
    fileChannel.write(in.nioBuffer())
    ctx.read()
  }
}

object SnapshotDataStreamHandler {
  val Name = "SnapshotDataStreamHandler#0"
}
