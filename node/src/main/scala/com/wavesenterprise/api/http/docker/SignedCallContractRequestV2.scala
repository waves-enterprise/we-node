package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.{CallContractTransaction, CallContractTransactionV2}
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[CallContractTransactionV2]] request
  */
case class SignedCallContractRequestV2(
    version: Int,
    senderPublicKey: String,
    contractId: String,
    contractVersion: Int,
    params: List[DataEntry[_]],
    fee: Long,
    timestamp: Long,
    proofs: List[String]
) extends BroadcastRequest {

  def toTx: Either[ValidationError, CallContractTransactionV2] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx         <- CallContractTransactionV2.create(sender, contractId, params, fee, timestamp, contractVersion, proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(CallContractTransaction.typeId.toInt))
}

object SignedCallContractRequestV2 {

  implicit val format: OFormat[SignedCallContractRequestV2] = Json.format

}
