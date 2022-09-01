package com.wavesenterprise.network

import cats.Show
import com.wavesenterprise.block.Block
import com.wavesenterprise.network.handshake.SignedHandshake
import com.wavesenterprise.network.message.MessageSpec._
import com.wavesenterprise.network.message._
import com.wavesenterprise.network.peers.PeerIdentityRequest
import com.wavesenterprise.settings.WEConfigReaders
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

@Sharable
class TrafficLogger(settings: TrafficLogger.Settings) extends ChannelDuplexHandler with ScorexLogging {

  import com.wavesenterprise.network.message.MessageSpec.specsByClasses

  private[network] val codeOf: AnyRef => Option[Byte] = {
    val aux: PartialFunction[AnyRef, Byte] = {
      case x: RawBytes                          => x.code
      case _: TransactionWithSize | Transaction => TransactionSpec.messageCode
      case _: BroadcastedTransaction            => BroadcastedTransactionSpec.messageCode
      case _: BigInt | _: LocalScoreChanged     => ScoreSpec.messageCode
      case _: Block | _: BlockForged            => BlockSpec.messageCode
      case _: HistoryBlock                      => HistoryBlockSpec.messageCode
      case _: PrivateDataResponse               => PrivateDataResponseSpec.messageCode
      case x: Message                           => specsByClasses(x.getClass).messageCode
      case _: SignedHandshake                   => HandshakeSpec.messageCode
      case _: PeerIdentityRequest               => PeerIdentitySpec.messageCode
    }

    aux.lift
  }

  override def write(ctx: ChannelHandlerContext, msg: AnyRef, promise: ChannelPromise): Unit = {
    codeOf(msg).filterNot(settings.ignoreTxMessages).foreach { code =>
      log.trace(s"'${id(ctx)}' <-- transmitted($code): $msg")
    }

    super.write(ctx, msg, promise)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
    codeOf(msg).filterNot(settings.ignoreRxMessages).foreach { code =>
      log.trace(s"'${id(ctx)}' --> received($code): $msg")
    }

    super.channelRead(ctx, msg)
  }
}

object TrafficLogger {

  val IgnoreNothing = Settings(Set.empty, Set.empty)

  case class Settings(ignoreTxMessages: Set[MessageCode], ignoreRxMessages: Set[MessageCode])

  object Settings extends WEConfigReaders {
    implicit val configReader: ConfigReader[Settings] = deriveReader
  }

  implicit val toPrintable: Show[Settings] = { x =>
    import x._
    s"""
       |ignoreTxMessages: [${ignoreTxMessages.mkString(", ")}]
       |ignoreRxMessages: [${ignoreRxMessages.mkString(", ")}]
     """.stripMargin
  }
}
