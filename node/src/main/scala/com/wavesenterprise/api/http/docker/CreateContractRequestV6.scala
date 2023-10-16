package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.CreateContractTransactionV6
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CreateContractTransactionV6]] request
  */
case class CreateContractRequestV6(
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
    password: Option[String] = None,
    isConfidential: Boolean,
    groupParticipants: List[String],
    groupOwners: List[String]
) extends UnsignedTxRequest
    with SponsoredFeesSupport {
  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(CreateContractTransactionV6.typeId.toInt))
}

object CreateContractRequestV6 {

  implicit val format: Format[CreateContractRequestV6] = Json.format

}
