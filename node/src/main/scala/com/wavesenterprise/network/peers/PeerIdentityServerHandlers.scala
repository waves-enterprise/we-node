package com.wavesenterprise.network.peers

import java.util

import com.wavesenterprise.api.http.service.PeersIdentityService
import com.wavesenterprise.utils.ScorexLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.{MessageToByteEncoder, ReplayingDecoder}

@Sharable
class PeerIdentityProcessingHandler(peersIdentityService: PeersIdentityService) extends ChannelInboundHandlerAdapter with ScorexLogging {

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
    msg match {
      case req: PeerIdentityRequest =>
        log.trace("Peer identity message processing started")
        val response = peersIdentityService.getIdentity(req.nodeAddress, req.nodeAddress.bytes.arr, req.signature) match {
          case Right((pka, certificates)) => SuccessPeerIdentityResponse(pka.publicKey, certificates)
          case Left(error)                => MessagePeerIdentityResponse(error.toString)
        }
        val future = ctx.writeAndFlush(response)
        future.addListener(ChannelFutureListener.CLOSE)
      case _ => super.channelRead(ctx, msg)
    }
  }
}

class PeerIdentityRequestDecoder extends ReplayingDecoder[Void] with ScorexLogging {

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    try {
      log.trace("Decoding peer identity request")
      val request = PeerIdentityRequest.decodeSigned(in)
      out.add(request)
      log.trace(s"Peer identity request decoded successfully: '$request'")
      ctx.pipeline.remove(this)
    } catch {
      case e: Exception =>
        log.error("Failed to decode peer identity request", e)
        ctx.channel.close()
    }
  }
}

/**
  * Encodes [[PeerIdentityResponse]] to output
  */
class PeerIdentityResponseEncoder extends MessageToByteEncoder[PeerIdentityResponse] with ScorexLogging {

  override def encode(ctx: ChannelHandlerContext, msg: PeerIdentityResponse, out: ByteBuf): Unit = {
    msg.encode(out)
    log.trace(s"Encoded peer identity response: '$msg'")
  }
}
