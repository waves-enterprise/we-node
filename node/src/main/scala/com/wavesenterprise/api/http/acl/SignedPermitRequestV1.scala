package com.wavesenterprise.api.http.acl

import cats.implicits._
import com.wavesenterprise.account.{AddressOrAlias, PublicKeyAccount}
import com.wavesenterprise.acl.{OpType, PermissionOp, Role}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.acl.PermitTransactionV1
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.json.{Format, Json}

case class SignedPermitRequestV1(senderPublicKey: String,
                                 target: String,
                                 role: String,
                                 opType: String,
                                 timestamp: Long,
                                 fee: Long,
                                 dueTimestamp: Option[Long],
                                 proofs: List[String])
    extends BroadcastRequest {

  def mkPermissionOp: Either[ValidationError, PermissionOp] =
    for {
      r  <- Role.fromStr(role)
      op <- OpType.fromStr(opType)
    } yield PermissionOp(op, r, timestamp, dueTimestamp)

  def toTx: Either[ValidationError, PermitTransactionV1] =
    for {
      sender        <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      targetAddress <- AddressOrAlias.fromString(target).leftMap(ValidationError.fromCryptoError)
      permOp        <- mkPermissionOp
      proofBytes    <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      parsedProofs  <- Proofs.create(proofBytes)
      tx            <- PermitTransactionV1.create(sender, targetAddress, timestamp, fee, permOp, parsedProofs)
    } yield tx
}

object SignedPermitRequestV1 {
  implicit val format: Format[SignedPermitRequestV1] = Json.format
}
