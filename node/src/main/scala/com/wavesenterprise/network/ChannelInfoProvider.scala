package com.wavesenterprise.network

import io.netty.channel.Channel

import scala.jdk.CollectionConverters._

trait ChannelInfoProvider {

  protected def channelInfo(channel: Channel): String =
    "Attributes:\n" + channel.config.getOptions.asScala.view.map { case (k, v) => s"$k: $v" }.mkString("\n")
}
