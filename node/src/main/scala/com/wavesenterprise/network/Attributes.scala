package com.wavesenterprise.network

import com.wavesenterprise.settings.NodeMode
import io.netty.channel.Channel
import io.netty.util.AttributeKey
import com.wavesenterprise.utils.ScorexLogging

object Attributes extends ScorexLogging {
  val NodeNameAttributeKey: AttributeKey[String] = AttributeKey.newInstance[String]("name")

  val NodeAttributesKey: AttributeKey[Unit]                   = AttributeKey.newInstance[Unit]("node_attributes")
  val NodeModeAttribute: AttributeKey[NodeMode]               = AttributeKey.newInstance[NodeMode]("node_mode")
  val MinerAttribute: AttributeKey[Unit]                      = AttributeKey.newInstance("miner")
  val ValidatorAttribute: AttributeKey[Unit]                  = AttributeKey.newInstance("validator")
  val TlsAttribute: AttributeKey[Unit]                        = AttributeKey.newInstance("tls")
  val SeparateBlockAndTxMessagesAttribute: AttributeKey[Unit] = AttributeKey.newInstance("separate_block_and_tx_messages")
  val PeerIdentityWithCertsSupport: AttributeKey[Unit]        = AttributeKey.newInstance("peer_identity_with_certs_support")

  implicit class ChannelAttrOps(private val ch: Channel) extends AnyVal {
    def setAttrWithLogging[T](attrKey: AttributeKey[T], value: T): Unit = {
      ch.attr(attrKey).set(value)
      log.trace(s"Setting attribute '$attrKey' for '${id(ch)}'")
    }
  }
}
