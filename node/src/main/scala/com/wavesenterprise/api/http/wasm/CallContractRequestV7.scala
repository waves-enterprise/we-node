package com.wavesenterprise.api.http.wasm

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.CallContractTransactionV7
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

case class CallContractRequestV7(
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
    password: Option[String] = None,
    inputCommitment: Option[String] = None,
    contractEngine: String,
    callFunc: Option[String]
) extends UnsignedTxRequest
    with SponsoredFeesSupport {

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(CallContractTransactionV7.typeId.toInt))

}

object CallContractRequestV7 {

  implicit val format: OFormat[CallContractRequestV7] = Json.format

}
