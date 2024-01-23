package com.wavesenterprise.api.http.wasm

import cats.implicits._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.docker.{ContractApiVersion, StoredContract}
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import com.wavesenterprise.transaction.docker.{CreateContractTransaction, CreateContractTransactionV7}
import com.wavesenterprise.transaction.{AssetId, AtomicBadge, Proofs, ValidationError}
import com.wavesenterprise.utils.Base64
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[CreateContractTransactionV7]] request
  */
case class SignedCreateContractRequestV7(
    senderPublicKey: String,
    storedContract: StoredContract,
    apiVersion: ContractApiVersion,
    contractName: String,
    params: List[DataEntry[_]],
    fee: Long,
    feeAssetId: Option[AssetId],
    atomicBadge: Option[AtomicBadge],
    validationPolicy: ValidationPolicy,
    payments: List[ContractTransferInV1],
    isConfidential: Boolean,
    groupParticipants: Set[Address],
    groupOwners: Set[Address],
    timestamp: Long,
    proofs: List[String]
) extends BroadcastRequest {

  def toTx: Either[ValidationError, CreateContractTransactionV7] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      tx <- CreateContractTransactionV7.create(
        sender = sender,
        contractName = contractName,
        apiVersion = apiVersion,
        params = params,
        fee = fee,
        timestamp = timestamp,
        feeAssetId = feeAssetId,
        atomicBadge = atomicBadge,
        validationPolicy = validationPolicy,
        payments = payments,
        isConfidential = isConfidential,
        groupParticipants = groupParticipants,
        groupOwners = groupOwners,
        storedContract = storedContract,
        proofs = proofs
      )
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("version" -> JsNumber(7)) + ("type" -> JsNumber(CreateContractTransaction.typeId.toInt))
}

object SignedCreateContractRequestV7 {

  implicit val format: OFormat[SignedCreateContractRequestV7] = Json.format

}
