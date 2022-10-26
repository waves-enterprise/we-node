package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.CallContractTransactionV5
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CallContractTransactionV5]] request
  */
case class CallContractRequestV5(
    version: Int,
    sender: String,
    contractId: String,
    contractVersion: Int,
    params: List[DataEntry[_]],
    payments: List[ContractTransferInV1],
    fee: Long,
    feeAssetId: Option[String],
    timestamp: Option[Long] = None,
    atomicBadge: Option[AtomicBadge],
    password: Option[String] = None
) extends UnsignedTxRequest
    with SponsoredFeesSupport {
  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(CallContractTransactionV5.typeId.toInt))
}

object CallContractRequestV5 {

  implicit val format: OFormat[CallContractRequestV5] = Json.format

}
