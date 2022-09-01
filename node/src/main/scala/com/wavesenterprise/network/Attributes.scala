package com.wavesenterprise.network

import com.wavesenterprise.settings.NodeMode
import io.netty.channel.Channel
import io.netty.util.AttributeKey
import com.wavesenterprise.utils.ScorexLogging

object Attributes extends ScorexLogging {
  val NodeNameAttributeKey: AttributeKey[String]              = AttributeKey.newInstance[String]("name")
  val NetworkMessageShaChecksumAttribute: AttributeKey[Unit]  = AttributeKey.newInstance[Unit]("messages_sha_checksum")
  val PrivacyProtocolExtensionV1Attribute: AttributeKey[Unit] = AttributeKey.newInstance[Unit]("privacy_protocol_extension_v1")
  val HistoryReplierExtensionV1Attribute: AttributeKey[Unit]  = AttributeKey.newInstance[Unit]("history_replier_extension_v1")
  val NodeAttributesKey: AttributeKey[Unit]                   = AttributeKey.newInstance[Unit]("node_attributes")
  val NodeModeAttribute: AttributeKey[NodeMode]               = AttributeKey.newInstance[NodeMode]("node_mode")
  val MinerAttrubute: AttributeKey[Unit]                      = AttributeKey.newInstance("miner")
  val ValidatorAttribute: AttributeKey[Unit]                  = AttributeKey.newInstance("validator")
  val TlsAttribute: AttributeKey[Unit]                        = AttributeKey.newInstance("tls")
  val SeparateBlockAndTxMessagesAttribute: AttributeKey[Unit] = AttributeKey.newInstance("separate_block_and_tx_messages")

  implicit class ChannelAttrOps(private val ch: Channel) extends AnyVal {
    def setAttrWithLogging[T](attrKey: AttributeKey[T], value: T): Unit = {
      ch.attr(attrKey).set(value)
      log.trace(s"Setting attribute '$attrKey' for '${id(ch)}'")
    }
  }
}
