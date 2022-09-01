package com.wavesenterprise.api.http.acl

import play.api.libs.json.{Format, Json}

case class PermissionsForAddressesReq(addresses: Seq[String], timestamp: Long)

object PermissionsForAddressesReq {
  implicit val format: Format[PermissionsForAddressesReq] = Json.format
}
