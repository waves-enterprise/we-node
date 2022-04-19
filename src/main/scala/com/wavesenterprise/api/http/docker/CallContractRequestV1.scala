package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.state.DataEntry
import play.api.libs.json.{Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CallContractTransactionV1]] request
  */
case class CallContractRequestV1(sender: String,
                                 contractId: String,
                                 params: List[DataEntry[_]],
                                 fee: Long,
                                 timestamp: Option[Long] = None,
                                 password: Option[String] = None)
    extends UnsignedTxRequest {}

object CallContractRequestV1 {

  implicit val format: OFormat[CallContractRequestV1] = Json.format

}
