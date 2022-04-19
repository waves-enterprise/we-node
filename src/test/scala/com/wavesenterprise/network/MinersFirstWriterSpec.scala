package com.wavesenterprise.network

import com.wavesenterprise.network.Attributes.MinerAttrubute
import com.wavesenterprise.network.peers.MinersFirstWriter.WriteResult
import com.wavesenterprise.network.peers.{ActivePeerConnections, MinersFirstWriter}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelId, ChannelOutboundHandlerAdapter, ChannelPromise}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.{DefaultThreadFactory, GlobalEventExecutor}
import org.scalatest.{FreeSpec, Matchers}

import scala.collection.mutable.ListBuffer

class MinersFirstWriterSpec extends FreeSpec with Matchers {

  "broadcast first to miners" - {
    "should first send a message to having miner's permission then others" in {
      val message   = "test"
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
      minerChannels.foreach(_.attr(MinerAttrubute).set(()))

      val allChannels = notMinerChannels ++ minerChannels

      allChannels.foreach { ch =>
        bossGroup.register(ch)
        connections.add(ch)
      }
      val minersFirstWriter = new ActivePeerConnections with MinersFirstWriter {}
      val WriteResult(minersChGroup, notMinersGroup) =
        minersFirstWriter.writeToRandomSubGroupMinersFirst(message, channelsGroup = connections, maxChannelCount = 10)
      minersChGroup.await(3000)
      notMinersGroup.await(3000)

      val (firstMiners, restMiners) = received.span(_ >= 50)
      restMiners.forall(_ < 50) shouldBe true
      firstMiners.toSet.subsetOf(minerIds) shouldBe true
      restMiners.toSet.subsetOf(notMinerIds) shouldBe true

    }
  }

}
