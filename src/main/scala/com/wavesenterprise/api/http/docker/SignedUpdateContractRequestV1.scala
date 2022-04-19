package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.{UpdateContractTransaction, UpdateContractTransactionV1}
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[UpdateContractTransactionV1]] request
  */
case class SignedUpdateContractRequestV1(senderPublicKey: String,
                                         contractId: String,
                                         image: String,
                                         imageHash: String,
                                         fee: Long,
                                         timestamp: Long,
                                         proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, UpdateContractTransactionV1] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx         <- UpdateContractTransactionV1.create(sender, contractId, image, imageHash, fee, timestamp, proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(UpdateContractTransaction.typeId.toInt))
}

object SignedUpdateContractRequestV1 {

  implicit val format: OFormat[SignedUpdateContractRequestV1] = Json.format

}
