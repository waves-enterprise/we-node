package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.CreateContractTransactionV5
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CreateContractTransactionV5]] request
  */
case class CreateContractRequestV5(
    version: Int,
    sender: String,
    image: String,
    imageHash: String,
    contractName: String,
    params: List[DataEntry[_]],
    payments: List[ContractTransferInV1],
    fee: Long,
    feeAssetId: Option[String],
    timestamp: Option[Long] = None,
    atomicBadge: Option[AtomicBadge],
    validationPolicy: ValidationPolicy,
    apiVersion: ContractApiVersion,
    password: Option[String] = None
) extends UnsignedTxRequest
    with SponsoredFeesSupport {
  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(CreateContractTransactionV5.typeId.toInt))
}

object CreateContractRequestV5 {

  implicit val format: Format[CreateContractRequestV5] = Json.format

}
