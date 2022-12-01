package com.wavesenterprise.network

import com.wavesenterprise.network.Attributes.{MinerAttribute, SeparateBlockAndTxMessagesAttribute}
import com.wavesenterprise.network.peers.MinersFirstWriter.WriteResult
import com.wavesenterprise.network.peers.{ActivePeerConnections, MinersFirstWriter}
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.{Transaction, TransactionParser}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel._
import io.netty.util.concurrent.{DefaultThreadFactory, GlobalEventExecutor}
import monix.eval.Coeval
import org.scalamock.scalatest.MockFactory
import play.api.libs.json.JsObject

import scala.collection.mutable.ListBuffer
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MinersFirstWriterSpec extends AnyFreeSpec with Matchers with MockFactory {

  "broadcast first to miners" - {
    "should first send a message to having miner's permission then others" in {
      val txMock = new Transaction {
        override val id: Coeval[ByteStr]            = Coeval(ByteStr.empty)
        override def builder: TransactionParser     = ???
        override def fee: Long                      = ???
        override def timestamp: Long                = ???
        override val bytes: Coeval[Array[Byte]]     = Coeval(Array[Byte]())
        override val bodyBytes: Coeval[Array[Byte]] = bytes
        override val json: Coeval[JsObject]         = Coeval(JsObject.empty)
      }

      val message   = BroadcastedTransaction(TransactionWithSize(0, txMock), CertChainStore.empty)
      val bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("nio-boss-group", true))

      val connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
      val received    = ListBuffer[Int]()

      def receiver(id: Int): Channel = new EmbeddedChannel(
        new ChannelId {
          override def asShortText(): String = asLongText()

          override def asLongText(): String = id.toString

          override def compareTo(o: ChannelId): Int = o.asLongText().toInt - id
        },
        new ChannelOutboundHandlerAdapter {
          override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
            received += id
            super.write(ctx, msg, promise)
          }
        }
      )

      val notMinerIds      = (0 to 49).toSet
      val notMinerChannels = notMinerIds.map(receiver)

      val minerIds      = (50 to 100).toSet
      val minerChannels = minerIds.map(receiver)
      minerChannels.foreach(_.attr(MinerAttribute).set(()))

      val allChannels = notMinerChannels ++ minerChannels

      allChannels.foreach { ch =>
        ch.attr(SeparateBlockAndTxMessagesAttribute).set(())
        bossGroup.register(ch)
        connections.add(ch)
      }
      val minersFirstWriter = new ActivePeerConnections(100) with MinersFirstWriter {}
      val WriteResult(groupFutures) =
        minersFirstWriter.writeToRandomSubGroupMinersFirst(message, Seq(), channelsGroup = connections, maxChannelCount = 10)

      groupFutures.takeRight(4).foreach(_.await(2000))

      val (miners, notMiners) = received.span(_ >= 50)
      notMiners.forall(_ < 50) shouldBe true
      miners.toSet.subsetOf(minerIds) shouldBe true
      notMiners.toSet.subsetOf(notMinerIds) shouldBe true

    }
  }

}
