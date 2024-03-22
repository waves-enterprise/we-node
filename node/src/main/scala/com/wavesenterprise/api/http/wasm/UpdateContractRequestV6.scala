package com.wavesenterprise.api.http.wasm

import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.docker.StoredContract
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.UpdateContractTransaction
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.UpdateContractTransactionV6]] request
  */
case class UpdateContractRequestV6(sender: String,
                                   contractId: String,
                                   storedContract: StoredContract,
                                   fee: Long,
                                   feeAssetId: Option[String],
                                   atomicBadge: Option[AtomicBadge] = None,
                                   validationPolicy: ValidationPolicy,
                                   groupParticipants: Set[Address],
                                   groupOwners: Set[Address],
                                   timestamp: Option[Long] = None,
                                   password: Option[String] = None)
    extends UnsignedTxRequest with SponsoredFeesSupport {

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(UpdateContractTransaction.typeId.toInt))
}

object UpdateContractRequestV6 {

  implicit val format: OFormat[UpdateContractRequestV6] = Json.format

}
