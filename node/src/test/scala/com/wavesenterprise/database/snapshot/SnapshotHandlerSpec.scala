package com.wavesenterprise.database.snapshot

import com.wavesenterprise.RxSetup
import com.wavesenterprise.network.id
import com.wavesenterprise.network.netty.handler.stream.{ChunkedNioFile, ChunkedWriteHandler, StreamHandlerBase, StreamReadProgressListener}
import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.concurrent.ScalaFutures

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class SnapshotHandlerSpec extends AnyFreeSpec with Matchers with ScalaFutures with RxSetup {

  private def fixture(test: (Path, Path) => Unit): Unit = {
    val bytes = randomBytes(65536)

    val input = Files.createTempFile("snapshot-input", ".zip")
    Files.write(input, bytes)

    val file = Files.createTempFile("snapshot", ".zip")

    try {
      test(input, file)
    } finally {
      Files.delete(input)
      Files.delete(file)
    }
  }

  "test packed snapshot writing and reading" in fixture { (input, file) =>
    val inputBytes = Files.readAllBytes(input)
    val total      = inputBytes.length

    val writeChannel = new EmbeddedChannel(new ChunkedWriteHandler())
    writeChannel.writeOutbound(new ChunkedNioFile(input.toFile)) shouldBe true
    writeChannel.finish() shouldBe true

    val fileChannel     = FileChannel.open(file, StandardOpenOption.APPEND)
    val readChannel     = new EmbeddedChannel()
    val listener        = new StreamReadProgressListener(s"${id(readChannel)} snapshot loading", maybeTotal = Some(total))
    val snapshotHandler = new SnapshotDataStreamHandler(writeChannel, fileChannel, listener)
    readChannel.pipeline().addFirst(snapshotHandler)

    val written = Iterator
      .continually(writeChannel.readOutbound[ByteBuf])
      .takeWhile(_ != null)
      .foldLeft(0) {
        case (size, buffer) =>
          val magicNumberSize = if (buffer.isReadable()) Integer.BYTES else 0
          val chunkSize       = buffer.readableBytes()
          readChannel.writeInbound(buffer)
          size + (chunkSize - magicNumberSize)
      }

    written shouldBe total
    listener.progress shouldBe total

    Await.result(snapshotHandler.start, 5.seconds)
    Await.result(snapshotHandler.completion, 5.seconds)

    fileChannel.close()
    Files.readAllBytes(file) shouldBe inputBytes
  }

  "test packed snapshot writing and reading (with offset)" in fixture { (input, file) =>
    val inputBytes = Files.readAllBytes(input)
    val total      = inputBytes.length

    val offset       = Random.nextInt(total)
    val inputChannel = FileChannel.open(input, StandardOpenOption.READ)
    val chunkedFile  = new ChunkedNioFile(inputChannel, offset, total - offset, StreamHandlerBase.DefaultChunkSize)

    val writeChannel = new EmbeddedChannel(new ChunkedWriteHandler())
    writeChannel.writeOutbound(chunkedFile) shouldBe true
    writeChannel.finish() shouldBe true

    val fileChannel = FileChannel.open(file, StandardOpenOption.APPEND)
    fileChannel.write(ByteBuffer.wrap(inputBytes.take(offset)))

    val readChannel     = new EmbeddedChannel()
    val listener        = new StreamReadProgressListener(s"${id(readChannel)} snapshot loading", offset, Some(total))
    val snapshotHandler = new SnapshotDataStreamHandler(writeChannel, fileChannel, listener)
    readChannel.pipeline().addFirst(snapshotHandler)

    val written = Iterator
      .continually(writeChannel.readOutbound[ByteBuf])
      .takeWhile(_ != null)
      .foldLeft(0) {
        case (size, buffer) =>
          val magicNumberSize = if (buffer.isReadable()) Integer.BYTES else 0
          val chunkSize       = buffer.readableBytes()
          readChannel.writeInbound(buffer)
          size + (chunkSize - magicNumberSize)
      }

    (written + offset) shouldBe total
    listener.progress shouldBe total

    Await.result(snapshotHandler.start, 5.seconds)
    Await.result(snapshotHandler.completion, 5.seconds)

    fileChannel.close()
    Files.readAllBytes(file) shouldBe inputBytes
  }
}
