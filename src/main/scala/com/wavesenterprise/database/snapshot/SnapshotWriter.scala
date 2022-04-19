package com.wavesenterprise.database.snapshot

import com.wavesenterprise.network.netty.handler.stream.{ChunkedNioFile, StreamHandlerBase, StreamWriteProgressListener}
import com.wavesenterprise.network.{SnapshotRequest, id, taskFromChannelFuture}
import io.netty.channel.Channel
import monix.eval.Task

import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}

case class PackedSnapshotInfo(path: Path, size: Long)

trait SnapshotWriter {

  protected def packedSnapshotTask: Task[PackedSnapshotInfo]

  protected def writeSnapshot(channel: Channel, request: SnapshotRequest): Task[Unit] = {
    for {
      packedSnapshot <- packedSnapshotTask
      _ <- taskFromChannelFuture {
        val PackedSnapshotInfo(path, size)  = packedSnapshot
        val SnapshotRequest(sender, offset) = request
        val fileChannel                     = FileChannel.open(path, StandardOpenOption.READ)
        val chunkedFile                     = new ChunkedNioFile(fileChannel, offset, size - offset, StreamHandlerBase.DefaultChunkSize)
        val future                          = channel.writeAndFlush(chunkedFile, channel.newProgressivePromise())
        future.addListener(new StreamWriteProgressListener(s"${id(future.channel())} snapshot uploading for '${sender.toAddress}'"))
      }
    } yield ()
  }
}
