package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.TransactionFactory.parseUniqueAddressSet
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import com.wavesenterprise.transaction.docker.{CreateContractTransaction, CreateContractTransactionV6}
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[CreateContractTransactionV6]] request
  */
case class SignedCreateContractRequestV6(version: Int,
                                         senderPublicKey: String,
                                         image: String,
                                         imageHash: String,
                                         contractName: String,
                                         params: List[DataEntry[_]],
                                         payments: List[ContractTransferInV1],
                                         fee: Long,
                                         feeAssetId: Option[String],
                                         timestamp: Long,
                                         atomicBadge: Option[AtomicBadge],
                                         validationPolicy: ValidationPolicy,
                                         apiVersion: ContractApiVersion,
                                         isConfidential: Boolean,
                                         groupParticipants: List[String],
                                         groupOwners: List[String],
                                         proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, CreateContractTransactionV6] =
    for {
      sender               <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes           <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      feeAssetId           <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofs               <- Proofs.create(proofBytes)
      groupParticipantsSet <- parseUniqueAddressSet(groupParticipants, "Group participants")
      groupOwnersSet       <- parseUniqueAddressSet(groupOwners, "Group owners")
      tx <- CreateContractTransactionV6.create(
        sender = sender,
        image = image,
        imageHash = imageHash,
        contractName = contractName,
        params = params,
        fee = fee,
        timestamp = timestamp,
        feeAssetId = feeAssetId,
        atomicBadge = atomicBadge,
        validationPolicy = validationPolicy,
        apiVersion = apiVersion,
        payments = payments,
        isConfidential = isConfidential,
        groupParticipants = groupParticipantsSet,
        groupOwners = groupOwnersSet,
        proofs = proofs
      )
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(CreateContractTransaction.typeId.toInt))
}

object SignedCreateContractRequestV6 {

  implicit val format: OFormat[SignedCreateContractRequestV6] = Json.format

}
