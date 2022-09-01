package com.wavesenterprise.network.peers

import io.netty.channel.Channel
import io.netty.channel.group.ChannelMatcher

class CompositeChannelMatcher(channelMatchers: ChannelMatcher*) extends ChannelMatcher {
  override def matches(channel: Channel): Boolean = {
    channelMatchers.forall(_.matches(channel))
  }
}
