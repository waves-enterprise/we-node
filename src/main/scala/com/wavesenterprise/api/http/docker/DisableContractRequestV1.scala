package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.UnsignedTxRequest
import play.api.libs.json.{Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.DisableContractTransactionV1]] request
  */
case class DisableContractRequestV1(sender: String, contractId: String, fee: Long, timestamp: Option[Long] = None, password: Option[String] = None)
    extends UnsignedTxRequest

object DisableContractRequestV1 {

  implicit val format: OFormat[DisableContractRequestV1] = Json.format

}
