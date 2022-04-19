package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.{UpdateContractTransaction, UpdateContractTransactionV4}
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[UpdateContractTransactionV4]] request
  */
case class SignedUpdateContractRequestV4(senderPublicKey: String,
                                         contractId: String,
                                         image: String,
                                         imageHash: String,
                                         fee: Long,
                                         timestamp: Long,
                                         feeAssetId: Option[String],
                                         atomicBadge: Option[AtomicBadge],
                                         validationPolicy: ValidationPolicy,
                                         apiVersion: ContractApiVersion,
                                         proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, UpdateContractTransactionV4] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx <- UpdateContractTransactionV4.create(
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
        proofs = proofs
      )
    } yield tx

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(UpdateContractTransaction.typeId.toInt)) + ("version" -> JsNumber(4))
}

object SignedUpdateContractRequestV4 {

  implicit val format: OFormat[SignedUpdateContractRequestV4] = Json.format

}
