package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.transaction.{Proofs, RegisterNodeTransactionV1, ValidationError}
import play.api.libs.json.{Json, OFormat}

case class SignedRegisterNodeRequest(senderPublicKey: String,
                                     targetPubKey: String,
                                     opType: String,
                                     nodeName: Option[String],
                                     timestamp: Long,
                                     fee: Long,
                                     proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, RegisterNodeTransactionV1] = {
    for {
      senderPublicKey <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      targetPublicKey <- PublicKeyAccount.fromBase58String(targetPubKey).leftMap(ValidationError.fromCryptoError)
      opType          <- OpType.fromStr(opType)
      proofBytes      <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      parsedProofs    <- Proofs.create(proofBytes)
      tx              <- RegisterNodeTransactionV1.create(senderPublicKey, targetPublicKey, nodeName, opType, timestamp, fee, parsedProofs)
    } yield tx
  }
}

object SignedRegisterNodeRequest {
  implicit val format: OFormat[SignedRegisterNodeRequest] = Json.format
}
