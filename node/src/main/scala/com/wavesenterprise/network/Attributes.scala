package com.wavesenterprise.network

import com.github.ghik.silencer.silent
import com.wavesenterprise.settings.NodeMode
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel
import io.netty.util.AttributeKey

object Attributes extends ScorexLogging {
  val NodeNameAttributeKey: AttributeKey[String]     = AttributeKey.newInstance[String]("name")
  val OwnerAddressAttributeKey: AttributeKey[String] = AttributeKey.newInstance[String]("owner_address")

  val MinerAttribute: AttributeKey[Unit]                      = AttributeKey.newInstance("miner")
  val NodeAttributesKey: AttributeKey[Unit]                   = AttributeKey.newInstance[Unit]("node_attributes")
  val NodeModeAttribute: AttributeKey[NodeMode]               = AttributeKey.newInstance[NodeMode]("node_mode")
  val PeerIdentityWithCertsSupport: AttributeKey[Unit]        = AttributeKey.newInstance("peer_identity_with_certs_support")
  val SeparateBlockAndTxMessagesAttribute: AttributeKey[Unit] = AttributeKey.newInstance("separate_block_and_tx_messages")
  val TlsAttribute: AttributeKey[Unit]                        = AttributeKey.newInstance("tls")
  val ValidatorAttribute: AttributeKey[Unit]                  = AttributeKey.newInstance("validator")

  implicit class ChannelAttrOps(private val ch: Channel) extends AnyVal {
    def setAttrWithLogging[T](attrKey: AttributeKey[T], value: T): Unit =
      if (ch.hasAttr(attrKey)) log.trace(s"Attribute '$attrKey' for '${id(ch)}' is already set")
      else {
        setAttr(attrKey, value)
        log.trace(s"Setting attribute '$attrKey' for '${id(ch)}'")
      }

    def removeAttrWithLogging[T](attrKey: AttributeKey[T]): Unit =
      if (ch.hasAttr(attrKey)) {
        removeAttr(attrKey)
        log.trace(s"Removing attribute '$attrKey' for '${id(ch)}'")
      } else ()

    def setAttr[T](attrKey: AttributeKey[T], value: T): Unit = ch.attr(attrKey).set(value)

    @silent("deprecated")
    def removeAttr[T](attrKey: AttributeKey[T]): Unit = ch.attr(attrKey).remove()
  }
}
