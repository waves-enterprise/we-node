package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.docker.CallContractTransaction
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CallContractTransactionV2]] request
  */
case class CallContractRequestV2(version: Int,
                                 sender: String,
                                 contractId: String,
                                 contractVersion: Int,
                                 params: List[DataEntry[_]],
                                 fee: Long,
                                 timestamp: Option[Long] = None,
                                 password: Option[String] = None)
    extends UnsignedTxRequest {

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(CallContractTransaction.typeId.toInt))
}

object CallContractRequestV2 {

  implicit val format: OFormat[CallContractRequestV2] = Json.format

}
