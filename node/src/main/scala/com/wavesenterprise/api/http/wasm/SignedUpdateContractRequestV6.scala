package com.wavesenterprise.api.http.wasm

import cats.implicits._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.docker.{ContractApiVersion, StoredContract}
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.{GenericError, InvalidContractId}
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import com.wavesenterprise.transaction.docker.{UpdateContractTransaction, UpdateContractTransactionV6}
import com.wavesenterprise.transaction.{AssetId, AtomicBadge, Proofs, ValidationError}
import com.wavesenterprise.utils.Base64
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[UpdateContractTransactionV6]] request
  */
case class SignedUpdateContractRequestV6(senderPublicKey: String,
                                         contractId: String,
                                         storedContract: StoredContract,
                                         fee: Long,
                                         feeAssetId: Option[AssetId],
                                         atomicBadge: Option[AtomicBadge],
                                         validationPolicy: ValidationPolicy,
                                         payments: List[ContractTransferInV1],
                                         isConfidential: Boolean,
                                         groupParticipants: Set[Address],
                                         groupOwners: Set[Address],
                                         timestamp: Long,
                                         proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, UpdateContractTransactionV6] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx <- UpdateContractTransactionV6.create(
        sender = sender,
        contractId = contractId,
        storedContract = storedContract,
        fee = fee,
        timestamp = timestamp,
        feeAssetId = feeAssetId,
        atomicBadge = atomicBadge,
        validationPolicy = validationPolicy,
        groupParticipants = groupParticipants,
        groupOwners = groupOwners,
        proofs = proofs
      )
    } yield tx

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(UpdateContractTransaction.typeId.toInt)) + ("version" -> JsNumber(6))
}

object SignedUpdateContractRequestV6 {

  implicit val format: OFormat[SignedUpdateContractRequestV6] = Json.format

}
