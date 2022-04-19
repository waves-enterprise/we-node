package com.wavesenterprise.network

import com.wavesenterprise.network.Attributes._
import io.netty.util.AttributeKey

case class ProtocolFeature(id: Short, description: String, triggerAttribute: Option[AttributeKey[Unit]] = None)

object ProtocolFeature {
  val PeersHostnameSupport: ProtocolFeature =
    ProtocolFeature(1, "Share peer hostnames", Some(PeersV2Attribute))
  val MultipleExtBlocksRequest: ProtocolFeature =
    ProtocolFeature(3, "Request multiple extension blocks at once", Some(MultipleExtBlocksAttribute))
  val NetworkMessageShaChecksum: ProtocolFeature =
    ProtocolFeature(4, "Use SHA-256 for network messages' checksum calculation", Some(NetworkMessageShaChecksumAttribute))
  val PrivacyProtocolExtensionV1: ProtocolFeature =
    ProtocolFeature(5, "Privacy protocol extension v1", Some(PrivacyProtocolExtensionV1Attribute))
  val NodeAttributesFeature: ProtocolFeature =
    ProtocolFeature(6, "Support node attributes", Some(NodeAttributesKey))
  val HistoryReplierExtensionV1: ProtocolFeature =
    ProtocolFeature(7, "History protocol extension v1", Some(HistoryReplierExtensionV1Attribute))
}
