package com.wavesenterprise.network.handshake

import com.wavesenterprise.network.message.MessageSpec._
import com.wavesenterprise.network.peers.PeerIdentityRequestDecoder
import com.wavesenterprise.network.{MetaMessageCodec, closeChannel}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ReplayingDecoder

import java.util

/**
  * Adds one of [[PeerIdentityRequestDecoder]], [[HandshakeDecoder]], [[GenesisSnapshotRequestSpec]],
  * [[SnapshotRequestSpec]] to pipeline depending on first byte in message
  */
class HandshakeFreeMessagesDecoder() extends ReplayingDecoder[Void] with ScorexLogging {

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val messageCodeByte = in.getByte(0)
    if (messageCodeByte == PeerIdentitySpec.messageCode || SignedHandshake.AllTypeBytes.contains(messageCodeByte)) {
      val messageDecoder = messageCodeByte match {
        case PeerIdentitySpec.messageCode => new PeerIdentityRequestDecoder
        case _                            => new HandshakeDecoder()
      }
      ctx.pipeline().addAfter(ctx.name(), messageDecoder.getClass.getName, messageDecoder)
      if (in.isReadable) {
        out.add(in.readBytes(actualReadableBytes()))
      }
      if (SignedHandshake.AllTypeBytes.contains(messageCodeByte)) {
        ctx.pipeline().remove(this)
      }
    } else if (
      in.capacity() > 8 &&
      in.getInt(4) == MetaMessageCodec.Magic &&
      Set(GenesisSnapshotRequestSpec.messageCode, SnapshotRequestSpec.messageCode).contains(in.getByte(8))
    ) {
      ctx.pipeline().remove(this)
      if (in.isReadable) out.add(in.readBytes(actualReadableBytes()))
    } else {
      in.readByte()
      closeChannel(ctx.channel(), s"Unexpected message byte '$messageCodeByte' from '${ctx.channel().remoteAddress()}'")
    }
  }
}
