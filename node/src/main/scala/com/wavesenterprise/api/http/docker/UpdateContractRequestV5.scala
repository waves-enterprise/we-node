package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.UpdateContractTransaction
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.UpdateContractTransactionV5]] request
  */
case class UpdateContractRequestV5(sender: String,
                                   contractId: String,
                                   image: String,
                                   imageHash: String,
                                   fee: Long,
                                   feeAssetId: Option[String],
                                   timestamp: Option[Long] = None,
                                   atomicBadge: Option[AtomicBadge],
                                   validationPolicy: ValidationPolicy,
                                   apiVersion: ContractApiVersion,
                                   password: Option[String] = None,
                                   groupParticipants: List[String],
                                   groupOwners: List[String])
    extends UnsignedTxRequest
    with SponsoredFeesSupport {

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(UpdateContractTransaction.typeId.toInt))
}

object UpdateContractRequestV5 {

  implicit val format: OFormat[UpdateContractRequestV5] = Json.format

}
