package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.TransactionFactory.parseUniqueAddressSet
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.{UpdateContractTransaction, UpdateContractTransactionV5}
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[UpdateContractTransactionV5]] request
  */
case class SignedUpdateContractRequestV5(senderPublicKey: String,
                                         contractId: String,
                                         image: String,
                                         imageHash: String,
                                         fee: Long,
                                         timestamp: Long,
                                         feeAssetId: Option[String],
                                         atomicBadge: Option[AtomicBadge],
                                         validationPolicy: ValidationPolicy,
                                         apiVersion: ContractApiVersion,
                                         groupParticipants: List[String],
                                         groupOwners: List[String],
                                         proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, UpdateContractTransactionV5] =
    for {
      sender               <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      feeAssetId           <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofBytes           <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs               <- Proofs.create(proofBytes)
      contractId           <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      groupParticipantsSet <- parseUniqueAddressSet(groupParticipants, "Group participants")
      groupOwnersSet       <- parseUniqueAddressSet(groupOwners, "Group owners")
      tx <- UpdateContractTransactionV5.create(
        sender = sender,
        contractId = contractId,
        image = image,
        imageHash = imageHash,
        fee = fee,
        timestamp = timestamp,
        feeAssetId = feeAssetId,
        atomicBadge = atomicBadge,
        validationPolicy = validationPolicy,
        apiVersion = apiVersion,
        groupParticipants = groupParticipantsSet,
        groupOwners = groupOwnersSet,
        proofs = proofs
      )
    } yield tx

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(UpdateContractTransaction.typeId.toInt)) + ("version" -> JsNumber(4))
}

object SignedUpdateContractRequestV5 {

  implicit val format: OFormat[SignedUpdateContractRequestV5] = Json.format

}
