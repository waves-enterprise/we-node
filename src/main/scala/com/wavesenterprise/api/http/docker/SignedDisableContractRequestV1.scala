package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.{DisableContractTransaction, DisableContractTransactionV1}
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[DisableContractTransactionV1]] request
  */
case class SignedDisableContractRequestV1(senderPublicKey: String, contractId: String, fee: Long, timestamp: Long, proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, DisableContractTransactionV1] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx         <- DisableContractTransactionV1.create(sender, contractId, fee, timestamp, proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(DisableContractTransaction.typeId.toInt))
}

object SignedDisableContractRequestV1 {

  implicit val format: OFormat[SignedDisableContractRequestV1] = Json.format

}
