package com.wavesenterprise.network

import com.wavesenterprise.network.message.MessageSpec.TransactionSpec
import com.wavesenterprise.settings.NodeMode
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext}
import monix.execution.Scheduler
import monix.reactive.Observable

@Sharable
class DiscardingHandler(mode: NodeMode, blockchainReadiness: Observable[Boolean], implicit val scheduler: Scheduler)
    extends ChannelDuplexHandler
    with ScorexLogging {

  private val lastReadiness = lastObserved(blockchainReadiness)

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case _: TransactionWithSize | _: PrivateDataRequest | _: PrivateDataResponse if mode == NodeMode.Watcher =>
    case RawBytes(code, _) if code == TransactionSpec.messageCode && !lastReadiness().contains(true) =>
      log.trace(s"'${id(ctx)}' Discarding incoming message $code")
    case _ => super.channelRead(ctx, msg)
  }
}
