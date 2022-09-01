package com.wavesenterprise.api.http.acl

import cats.implicits._
import com.wavesenterprise.account.{AddressOrAlias, PublicKeyAccount}
import com.wavesenterprise.acl.{OpType, PermissionOp, Role}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.acl.PermitTransactionV2
import com.wavesenterprise.transaction.{AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{Format, Json}

case class SignedPermitRequestV2(senderPublicKey: String,
                                 target: String,
                                 role: String,
                                 opType: String,
                                 timestamp: Long,
                                 fee: Long,
                                 dueTimestamp: Option[Long],
                                 atomicBadge: Option[AtomicBadge],
                                 proofs: List[String])
    extends BroadcastRequest {

  def mkPermissionOp: Either[ValidationError, PermissionOp] =
    for {
      r  <- Role.fromStr(role)
      op <- OpType.fromStr(opType)
    } yield PermissionOp(op, r, timestamp, dueTimestamp)

  def toTx: Either[ValidationError, PermitTransactionV2] =
    for {
      sender        <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      targetAddress <- AddressOrAlias.fromString(target).leftMap(ValidationError.fromCryptoError)
      permOp        <- mkPermissionOp
      proofBytes    <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      parsedProofs  <- Proofs.create(proofBytes)
      tx            <- PermitTransactionV2.create(sender, targetAddress, timestamp, fee, permOp, atomicBadge, parsedProofs)
    } yield tx
}

object SignedPermitRequestV2 {
  implicit val format: Format[SignedPermitRequestV2] = Json.format
}
