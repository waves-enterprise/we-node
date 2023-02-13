package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.transaction.{AtomicBadge, Proofs, RegisterNodeTransactionV2, ValidationError}
import play.api.libs.json.{Json, OFormat}

case class SignedRegisterNodeRequestV2(senderPublicKey: String,
                                       targetPubKey: String,
                                       opType: String,
                                       nodeName: Option[String],
                                       timestamp: Long,
                                       fee: Long,
                                       atomicBadge: Option[AtomicBadge],
                                       proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, RegisterNodeTransactionV2] = {
    for {
      senderPublicKey <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      targetPublicKey <- PublicKeyAccount.fromBase58String(targetPubKey).leftMap(ValidationError.fromCryptoError)
      opType          <- OpType.fromStr(opType)
      proofBytes      <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      parsedProofs    <- Proofs.create(proofBytes)
      tx              <- RegisterNodeTransactionV2.create(senderPublicKey, targetPublicKey, nodeName, opType, timestamp, fee, atomicBadge, parsedProofs)
    } yield tx
  }
}

object SignedRegisterNodeRequestV2 {
  implicit val format: OFormat[SignedRegisterNodeRequestV2] = Json.format
}
