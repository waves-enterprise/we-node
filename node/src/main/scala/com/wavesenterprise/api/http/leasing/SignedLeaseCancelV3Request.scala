package com.wavesenterprise.api.http.leasing

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.TransactionParsers.SignatureStringLength
import com.wavesenterprise.transaction.lease.LeaseCancelTransactionV3
import com.wavesenterprise.transaction.{AtomicBadge, Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json._

case class SignedLeaseCancelV3Request(version: Byte,
                                      chainId: Byte,
                                      senderPublicKey: String,
                                      leaseId: String,
                                      timestamp: Long,
                                      fee: Long,
                                      atomicBadge: Option[AtomicBadge],
                                      proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, LeaseCancelTransactionV3] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _leaseTx    <- parseBase58(leaseId, "invalid.leaseTx", SignatureStringLength)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _           <- LeaseCancelTransactionV3.ensureSupportedVersion(version)
      _t          <- LeaseCancelTransactionV3.create(chainId, _sender, fee, timestamp, _leaseTx, atomicBadge, _proofs)
    } yield _t
}
object SignedLeaseCancelV3Request {
  implicit val reads: Reads[SignedLeaseCancelV3Request] = (
    (JsPath \ "version").read[Byte] and
      (JsPath \ "chainId").read[Byte] and
      (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "leaseId").read[String] and
      (JsPath \ "timestamp").read[Long] and
      (JsPath \ "fee").read[Long] and
      (JsPath \ "atomicBadge").readNullable[AtomicBadge] and
      (JsPath \ "proofs").read[List[String]]
  )(SignedLeaseCancelV3Request.apply _)

  implicit val writes: OWrites[SignedLeaseCancelV3Request] = Json.writes[SignedLeaseCancelV3Request]
}
