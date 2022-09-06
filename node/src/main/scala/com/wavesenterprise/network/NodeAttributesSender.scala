package com.wavesenterprise.network

import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.network.handshake.SignedHandshake
import com.wavesenterprise.settings.NetworkSettings
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

@Sharable
class NodeAttributesSender(ownerKey: PrivateKeyAccount, settings: NetworkSettings, p2pTlsEnabled: Boolean = false)
    extends ChannelInboundHandlerAdapter
    with ScorexLogging {

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
    msg match {
      case _: SignedHandshake =>
        val channel = ctx.channel
        if (channel.isOpen) {
          val nodeAttributes = NodeAttributes(settings.mode, p2pTlsEnabled, ownerKey)
          val attributes     = RawAttributes.createAndSign(ownerKey, nodeAttributes)
          channel.writeAndFlush(attributes)
          log.debug(show"$attributes sent to '${id(channel)}'")
        }
        super.channelRead(ctx, msg)
      case _ =>
        super.channelRead(ctx, msg)
    }
  }
}
