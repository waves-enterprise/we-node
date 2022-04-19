package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.docker.{CreateContractTransaction, CreateContractTransactionV1}
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[CreateContractTransactionV1]] request
  */
case class SignedCreateContractRequestV1(
    senderPublicKey: String,
    image: String,
    imageHash: String,
    contractName: String,
    params: List[DataEntry[_]],
    fee: Long,
    timestamp: Long,
    proofs: List[String]
) extends BroadcastRequest {

  def toTx: Either[ValidationError, CreateContractTransactionV1] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      tx         <- CreateContractTransactionV1.create(sender, image, imageHash, contractName, params, fee, timestamp, proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(CreateContractTransaction.typeId.toInt))
}

object SignedCreateContractRequestV1 {

  implicit val format: OFormat[SignedCreateContractRequestV1] = Json.format

}
