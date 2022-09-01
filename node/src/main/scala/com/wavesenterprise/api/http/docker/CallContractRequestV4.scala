package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.CallContractTransactionV4
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CallContractTransactionV4]] request
  */
case class CallContractRequestV4(
    version: Int,
    sender: String,
    contractId: String,
    contractVersion: Int,
    params: List[DataEntry[_]],
    fee: Long,
    feeAssetId: Option[String],
    timestamp: Option[Long] = None,
    atomicBadge: Option[AtomicBadge],
    password: Option[String] = None
) extends UnsignedTxRequest
    with SponsoredFeesSupport {
  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(CallContractTransactionV4.typeId.toInt))
}

object CallContractRequestV4 {

  implicit val format: OFormat[CallContractRequestV4] = Json.format

}
