package com.wavesenterprise.network.peers

import com.wavesenterprise.account.Address
import com.wavesenterprise.network.Attributes.{MinerAttribute, SeparateBlockAndTxMessagesAttribute}
import com.wavesenterprise.network.{BroadcastedTransaction, RawBytes, taskFromChannelGroupFuture}
import io.netty.channel.Channel
import io.netty.channel.group.{ChannelGroupFuture, ChannelMatcher, DefaultChannelGroup}
import monix.eval.Task

import scala.collection.JavaConverters._
import scala.util.Random

trait MinersFirstWriter { self: ActivePeerConnections =>
  import MinersFirstWriter._

  def writeMsgMinersFirst(broadcastedTx: BroadcastedTransaction,
                          preferredMiners: Seq[Address],
                          matcher: ChannelMatcher = { _ =>
                            true
                          },
                          channelsGroup: DefaultChannelGroup = connectedChannels): WriteResult = {

    val preferredMinersMatcher = new ChannelMatcher {
      override def matches(channel: Channel): Boolean = {
        matcher.matches(channel) && addressForChannel(channel).exists(preferredMiners.contains)
      }
    }

    val minerMatcher = new ChannelMatcher {
      override def matches(channel: Channel): Boolean = {
        matcher.matches(channel) && channel.hasAttr(MinerAttribute) && !addressForChannel(channel).exists(preferredMiners.contains)
      }
    }

    val notMinerMatcher = new ChannelMatcher {
      override def matches(channel: Channel): Boolean =
        matcher.matches(channel) && !channel.hasAttr(MinerAttribute) && !addressForChannel(channel).exists(preferredMiners.contains)
    }

    val separateBlockAndTxMatcher = new ChannelMatcher {
      override def matches(channel: Channel): Boolean =
        channel.hasAttr(SeparateBlockAndTxMessagesAttribute)
    }

    val olderProtocolMatcher = new ChannelMatcher {
      override def matches(channel: Channel): Boolean =
        !channel.hasAttr(SeparateBlockAndTxMessagesAttribute)
    }

    val olderProtocolTx = RawBytes.from(broadcastedTx.tx)

    val groupFutures = Seq(
      Option(preferredMiners)
        .filter(_.nonEmpty)
        .map(_ => channelsGroup.write(broadcastedTx, new CompositeChannelMatcher(preferredMinersMatcher, separateBlockAndTxMatcher))),
      Option(preferredMiners)
        .filter(_.nonEmpty)
        .map(_ => channelsGroup.write(olderProtocolTx, new CompositeChannelMatcher(preferredMinersMatcher, olderProtocolMatcher))),
      Some(channelsGroup.write(broadcastedTx, new CompositeChannelMatcher(minerMatcher, separateBlockAndTxMatcher))),
      Some(channelsGroup.write(olderProtocolTx, new CompositeChannelMatcher(minerMatcher, olderProtocolMatcher))),
      Some(channelsGroup.write(broadcastedTx, new CompositeChannelMatcher(notMinerMatcher, separateBlockAndTxMatcher))),
      Some(channelsGroup.write(olderProtocolTx, new CompositeChannelMatcher(notMinerMatcher, olderProtocolMatcher)))
    )

    WriteResult(groupFutures.flatten)
  }

  def writeToRandomSubGroupMinersFirst(message: BroadcastedTransaction,
                                       preferredMiners: Seq[Address],
                                       matcher: ChannelMatcher = { _ =>
                                         true
                                       },
                                       maxChannelCount: Int,
                                       channelsGroup: DefaultChannelGroup = connectedChannels): WriteResult = {
    val channels = Random
      .shuffle(channelsGroup.iterator().asScala.filter(matcher.matches).toSeq)
      .view
      .take(maxChannelCount)
      .toSet

    val randomMatcher = new ChannelMatcher {
      override def matches(channel: Channel): Boolean = channels.contains(channel)
    }
    writeMsgMinersFirst(message, preferredMiners, randomMatcher, channelsGroup)
  }

}

object MinersFirstWriter {

  case class WriteResult(groupFutures: Seq[ChannelGroupFuture]) {
    def allSuccessChannels: Task[Set[Channel]] =
      Task
        .parSequenceUnordered(groupFutures.map(taskFromChannelGroupFuture))
        .map(_.flatten.toSet)
  }
}
