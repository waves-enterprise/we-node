package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.{UpdateContractTransaction, UpdateContractTransactionV2}
import com.wavesenterprise.transaction.{AssetIdStringLength, Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[UpdateContractTransactionV2]] request
  */
case class SignedUpdateContractRequestV2(senderPublicKey: String,
                                         contractId: String,
                                         image: String,
                                         imageHash: String,
                                         fee: Long,
                                         timestamp: Long,
                                         feeAssetId: Option[String],
                                         proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, UpdateContractTransactionV2] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx         <- UpdateContractTransactionV2.create(sender, contractId, image, imageHash, fee, timestamp, feeAssetId, proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(UpdateContractTransaction.typeId.toInt)) + ("version" -> JsNumber(2))
}

object SignedUpdateContractRequestV2 {

  implicit val format: OFormat[SignedUpdateContractRequestV2] = Json.format

}
