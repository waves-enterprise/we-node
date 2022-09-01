package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.CreateContractTransactionV4
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CreateContractTransactionV4]] request
  */
case class CreateContractRequestV4(
    version: Int,
    sender: String,
    image: String,
    imageHash: String,
    contractName: String,
    params: List[DataEntry[_]],
    fee: Long,
    feeAssetId: Option[String],
    timestamp: Option[Long] = None,
    atomicBadge: Option[AtomicBadge],
    validationPolicy: ValidationPolicy,
    apiVersion: ContractApiVersion,
    password: Option[String] = None
) extends UnsignedTxRequest
    with SponsoredFeesSupport {
  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(CreateContractTransactionV4.typeId.toInt))
}

object CreateContractRequestV4 {

  implicit val format: Format[CreateContractRequestV4] = Json.format

}
