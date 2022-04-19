package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction._
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

case class SignedAtomicTransactionRequestV1(senderPublicKey: String, transactions: List[JsObject], timestamp: Long, proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, AtomicTransactionV1] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      innerTxs   <- transactions.traverse(txJson => SignedAtomicTransactionRequestV1.parseInnerTx(txJson))
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      tx         <- AtomicTransactionV1.create(sender, None, innerTxs, timestamp, proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(AtomicTransaction.typeId.toInt))
}

object SignedAtomicTransactionRequestV1 {

  def parseInnerTx(txJson: JsObject): Either[ValidationError, AtomicInnerTransaction] = {
    TransactionFactory.fromSignedRequest(txJson).flatMap {
      case tx: AtomicInnerTransaction => Right(tx)
      case _                          => Left(GenericError("Transaction does not support atomic containers"))
    }
  }

  implicit val format: OFormat[SignedAtomicTransactionRequestV1] = Json.format
}
