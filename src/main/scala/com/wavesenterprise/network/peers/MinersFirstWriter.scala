package com.wavesenterprise.network.peers

import com.wavesenterprise.network.Attributes.MinerAttrubute
import io.netty.channel.Channel
import io.netty.channel.group.{ChannelGroupFuture, ChannelMatcher, DefaultChannelGroup}

import scala.collection.JavaConverters._
import scala.util.Random

trait MinersFirstWriter { self: ActivePeerConnections =>
  import MinersFirstWriter._

  def writeMsgMinersFirst(message: Any, matcher: ChannelMatcher = { _ =>
    true
  }, channelsGroup: DefaultChannelGroup = connectedChannels): WriteResult = {

    val minerMatcher = new ChannelMatcher {
      override def matches(channel: Channel): Boolean = matcher.matches(channel) && channel.hasAttr(MinerAttrubute)
    }
    val notMinerMatcher = new ChannelMatcher {
      override def matches(channel: Channel): Boolean = matcher.matches(channel) && !channel.hasAttr(MinerAttrubute)
    }
    val minersChannelGroup    = channelsGroup.write(message, minerMatcher)
    val notMinersChannelGroup = channelsGroup.write(message, notMinerMatcher)
    WriteResult(minersChannelGroup, notMinersChannelGroup)
  }

  def writeToRandomSubGroupMinersFirst(message: Any, matcher: ChannelMatcher = { _ =>
    true
  }, maxChannelCount: Int, channelsGroup: DefaultChannelGroup = connectedChannels): WriteResult = {
    val channels = Random
      .shuffle(channelsGroup.iterator().asScala.filter(matcher.matches).toSeq)
      .view
      .take(maxChannelCount)
      .toSet

    val randomMatcher = new ChannelMatcher {
      override def matches(channel: Channel): Boolean = channels.contains(channel)
    }
    writeMsgMinersFirst(message, randomMatcher, channelsGroup)
  }

}

object MinersFirstWriter {
  case class WriteResult(minersChannelGroup: ChannelGroupFuture, notMinersChannelGroup: ChannelGroupFuture)
}
