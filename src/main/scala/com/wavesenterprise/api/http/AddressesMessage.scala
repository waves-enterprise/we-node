package com.wavesenterprise.api.http

import play.api.libs.json._

case class AddressesMessage(addresses: Array[String])

object AddressesMessage {

  implicit val addressesReads: Format[AddressesMessage] = Json.format
}
