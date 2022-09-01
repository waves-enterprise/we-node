package com.wavesenterprise.network

import java.util

import com.google.common.cache.Cache
import com.wavesenterprise.crypto
import com.wavesenterprise.network.message.MessageSpec.TransactionSpec
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ScorexLogging
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled._
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageCodec
import scorex.crypto.hash.Sha256

import scala.util.control.NonFatal

/**
  * Encodes meta information mostly related to network protocol and contains various checks:
  *  - magic number [[com.wavesenterprise.network.MetaMessageCodec.Magic]] at the start of each message
  *  - that there is a deserializer of type [[com.wavesenterprise.network.message.MessageSpec]] exist for given message
  *  - length of the message no more than required
  *  - message checksum
  * If all checks succeeded we are adding [[com.wavesenterprise.network.RawBytes]] to output
  * Also we maintain incoming txs cache to prevent txs duplicates
  **/
class MetaMessageCodec(receivedTxsCache: Cache[ByteStr, Object]) extends ByteToMessageCodec[RawBytes] with ScorexLogging {

  import MetaMessageCodec._
  import com.wavesenterprise.network.message.MessageSpec.specsByCodes

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit =
    try {
      require(in.readInt() == Magic, "Invalid magic number in network message")

      val messageCode = in.readByte()
      require(specsByCodes.contains(messageCode), s"Unexpected network message code $messageCode")

      val spec   = specsByCodes(messageCode)
      val length = in.readInt()
      require(length <= spec.maxLength, s"${spec.messageName} message length $length exceeds ${spec.maxLength}")

      val dataBytes = new Array[Byte](length)
      val pushToPipeline = length == 0 || {
        val declaredChecksum = in.readSlice(message.ChecksumLength)
        in.readBytes(dataBytes)
        val rawChecksum = if (ctx.channel().hasAttr(Attributes.NetworkMessageShaChecksumAttribute)) {
          Sha256.hash(dataBytes)
        } else {
          crypto.fastHash(dataBytes)
        }
        val actualChecksum = wrappedBuffer(rawChecksum, 0, message.ChecksumLength)

        require(declaredChecksum.equals(actualChecksum), "Invalid network message checksum")
        actualChecksum.release()

        spec != TransactionSpec || {
          receivedTxsCache.asMap().putIfAbsent(ByteStr(rawChecksum), dummy) == null
        }
      }

      if (pushToPipeline) {
        out.add(RawBytes(messageCode, dataBytes))
      }
    } catch {
      case NonFatal(e) =>
        log.warn(s"'${id(ctx)}' Malformed network message", e)
        closeChannel(ctx.channel, s"Malformed network message: $e")
    }

  override def encode(ctx: ChannelHandlerContext, msg: RawBytes, out: ByteBuf): Unit = {
    out.writeInt(Magic)
    out.writeByte(msg.code)
    if (msg.data.length > 0) {
      val rawChecksum = if (ctx.channel().hasAttr(Attributes.NetworkMessageShaChecksumAttribute)) {
        Sha256.hash(msg.data)
      } else {
        crypto.fastHash(msg.data)
      }

      out.writeInt(msg.data.length)
      out.writeBytes(rawChecksum, 0, message.ChecksumLength)
      out.writeBytes(msg.data)
    } else {
      out.writeInt(0)
    }
  }
}

object MetaMessageCodec {
  val Magic         = 0x12345678
  private val dummy = new Object()
}
