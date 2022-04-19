package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.docker.{CreateContractTransaction, CreateContractTransactionV2}
import com.wavesenterprise.transaction.{AssetIdStringLength, Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[CreateContractTransactionV2]] request
  */
case class SignedCreateContractRequestV2(version: Int,
                                         senderPublicKey: String,
                                         image: String,
                                         imageHash: String,
                                         contractName: String,
                                         params: List[DataEntry[_]],
                                         fee: Long,
                                         feeAssetId: Option[String],
                                         timestamp: Long,
                                         proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, CreateContractTransactionV2] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofs     <- Proofs.create(proofBytes)
      tx         <- CreateContractTransactionV2.create(sender, image, imageHash, contractName, params, fee, timestamp, feeAssetId, proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(CreateContractTransaction.typeId.toInt))
}

object SignedCreateContractRequestV2 {

  implicit val format: OFormat[SignedCreateContractRequestV2] = Json.format

}
