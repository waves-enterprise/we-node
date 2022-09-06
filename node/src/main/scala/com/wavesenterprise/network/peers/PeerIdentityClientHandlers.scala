package com.wavesenterprise.network.peers

import java.util
import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.crypto
import com.wavesenterprise.network.{Attributes, id}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.{MessageToByteEncoder, ReplayingDecoder}
import scorex.crypto.signatures.Signature

import scala.concurrent.Promise

class PeerIdentityClientHandler(ownerKey: PrivateKeyAccount, result: Promise[PeerIdentityResponse])
    extends ChannelInboundHandlerAdapter
    with ScorexLogging {

  private def createRequest(): PeerIdentityRequest = {
    val reqWOSignature = PeerIdentityRequest(ownerKey.toAddress, Signature(Array.empty[Byte]))
    val bytesWOSign    = PeerIdentityRequest.encodeUnsigned(reqWOSignature)
    val signature      = crypto.sign(ownerKey, bytesWOSign)
    reqWOSignature.copy(signature = signature)
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val request = createRequest()
    log.info(s"Sending peer identity request to '${id(ctx)}'")
    ctx.writeAndFlush(request).addListener { f: io.netty.util.concurrent.Future[Void] =>
      if (f.isSuccess) {
        log.trace(s"Successfully sent peer identity request to '${id(ctx)}")
      } else {
        result.failure(new RuntimeException(s"Couldn't send peer identity request to '${id(ctx)}'"))
        ctx.close()
      }
    }
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    msg match {
      case response: PeerIdentityResponse =>
        log.trace(s"Received peer identity response: '$response'")
        result.success(response)
      case _ =>
        result.failure(new RuntimeException("Unknown response. Closing channel"))
    }
    ctx.close()
  }
}

class PeerIdentityRequestEncoder extends MessageToByteEncoder[PeerIdentityRequest] with ScorexLogging {

  override def encode(ctx: ChannelHandlerContext, msg: PeerIdentityRequest, out: ByteBuf): Unit = {
    msg.encode(out)
    log.trace(s"Encoded peer identity request: '$msg'")
  }
}

class PeerIdentityResponseDecoder(result: Promise[PeerIdentityResponse]) extends ReplayingDecoder[Void] with ScorexLogging {

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    PeerIdentityResponse
      .decode(in, ctx.channel().hasAttr(Attributes.PeerIdentityWithCertsSupport))
      .map { response =>
        log.trace(s"Decoded peer identity response successfully: '$response'")
        out.add(response)
      }
      .leftMap { throwable =>
        result.failure(new RuntimeException("Can't decode peer identity response", throwable))
        ctx.close()
      }
  }
}
