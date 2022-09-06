package com.wavesenterprise.network

import com.wavesenterprise.network.Attributes._
import io.netty.util.AttributeKey

case class ProtocolFeature(id: Short, description: String, triggerAttribute: Option[AttributeKey[Unit]] = None)

object ProtocolFeature {
  val SeparateBlockAndTxMessages: ProtocolFeature =
    ProtocolFeature(8, "Split RawBytes into separate Block and Tx messages", Some(SeparateBlockAndTxMessagesAttribute))
}
