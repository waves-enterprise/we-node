package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.docker.{CreateContractTransaction, CreateContractTransactionV3}
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[CreateContractTransactionV3]] request
  */
case class SignedCreateContractRequestV3(
    version: Int,
    senderPublicKey: String,
    image: String,
    imageHash: String,
    contractName: String,
    params: List[DataEntry[_]],
    fee: Long,
    feeAssetId: Option[String],
    timestamp: Long,
    atomicBadge: Option[AtomicBadge],
    proofs: List[String]
) extends BroadcastRequest {

  def toTx: Either[ValidationError, CreateContractTransactionV3] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofs     <- Proofs.create(proofBytes)
      tx         <- CreateContractTransactionV3.create(sender, image, imageHash, contractName, params, fee, timestamp, feeAssetId, atomicBadge, proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(CreateContractTransaction.typeId.toInt))
}

object SignedCreateContractRequestV3 {

  implicit val format: OFormat[SignedCreateContractRequestV3] = Json.format

}
