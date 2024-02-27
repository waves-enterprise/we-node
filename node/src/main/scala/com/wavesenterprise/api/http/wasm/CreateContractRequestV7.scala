package com.wavesenterprise.api.http.wasm

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.docker.StoredContract
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.CreateContractTransactionV7
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CreateContractTransactionV7]] request
  */
case class CreateContractRequestV7(sender: String,
                                   contractName: String,
                                   storedContract: StoredContract,
                                   params: List[DataEntry[_]],
                                   payments: List[ContractTransferInV1],
                                   fee: Long,
                                   feeAssetId: Option[String],
                                   timestamp: Option[Long] = None,
                                   atomicBadge: Option[AtomicBadge],
                                   validationPolicy: ValidationPolicy,
                                   password: Option[String] = None,
                                   isConfidential: Boolean,
                                   groupParticipants: List[String],
                                   groupOwners: List[String]) extends UnsignedTxRequest with SponsoredFeesSupport {
  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(CreateContractTransactionV7.typeId.toInt))
}

object CreateContractRequestV7 {

  implicit val format: Format[CreateContractRequestV7] = Json.format

}
