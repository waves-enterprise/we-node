package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.UnsignedTxRequest
import play.api.libs.json.{Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.UpdateContractTransactionV1]] request
  */
case class UpdateContractRequestV1(sender: String,
                                   contractId: String,
                                   image: String,
                                   imageHash: String,
                                   fee: Long,
                                   timestamp: Option[Long] = None,
                                   password: Option[String] = None)
    extends UnsignedTxRequest

object UpdateContractRequestV1 {

  implicit val format: OFormat[UpdateContractRequestV1] = Json.format

}
