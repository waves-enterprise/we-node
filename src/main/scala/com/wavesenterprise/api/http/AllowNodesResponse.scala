package com.wavesenterprise.api.http

import play.api.libs.json.{Format, Json}

case class AddressWithPublicKey(address: String, publicKey: String)
object AddressWithPublicKey {
  implicit val format: Format[AddressWithPublicKey] = Json.format
}

case class AllowNodesResponse(allowedNodes: Seq[AddressWithPublicKey], timestamp: Long)
object AllowNodesResponse {
  implicit val format: Format[AllowNodesResponse] = Json.format
}
