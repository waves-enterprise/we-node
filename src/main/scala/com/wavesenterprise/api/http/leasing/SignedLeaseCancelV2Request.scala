package com.wavesenterprise.api.http.leasing

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.TransactionParsers.SignatureStringLength
import com.wavesenterprise.transaction.lease.LeaseCancelTransactionV2
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json._

case class SignedLeaseCancelV2Request(version: Byte,
                                      chainId: Byte,
                                      senderPublicKey: String,
                                      leaseId: String,
                                      timestamp: Long,
                                      proofs: List[String],
                                      fee: Long)
    extends BroadcastRequest {
  def toTx: Either[ValidationError, LeaseCancelTransactionV2] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _leaseTx    <- parseBase58(leaseId, "invalid.leaseTx", SignatureStringLength)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _           <- LeaseCancelTransactionV2.ensureSupportedVersion(version)
      _t          <- LeaseCancelTransactionV2.create(chainId, _sender, fee, timestamp, _leaseTx, _proofs)
    } yield _t
}
object SignedLeaseCancelV2Request {
  implicit val reads: Reads[SignedLeaseCancelV2Request] = (
    (JsPath \ "version").read[Byte] and
      (JsPath \ "chainId").read[Byte] and
      (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "leaseId").read[String] and
      (JsPath \ "timestamp").read[Long] and
      (JsPath \ "proofs").read[List[String]] and
      (JsPath \ "fee").read[Long]
  )(SignedLeaseCancelV2Request.apply _)

  implicit val writes = Json.writes[SignedLeaseCancelV2Request]
}
