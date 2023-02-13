package com.wavesenterprise.api.http.leasing

import cats.implicits._
import com.wavesenterprise.account.{AddressOrAlias, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.lease.LeaseTransactionV3
import com.wavesenterprise.transaction.{AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{Format, Json}

case class SignedLeaseV3Request(version: Byte,
                                senderPublicKey: String,
                                amount: Long,
                                fee: Long,
                                recipient: String,
                                timestamp: Long,
                                atomicBadge: Option[AtomicBadge],
                                proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, LeaseTransactionV3] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _recipient  <- AddressOrAlias.fromString(recipient).leftMap(ValidationError.fromCryptoError)
      _           <- LeaseTransactionV3.ensureSupportedVersion(version)
      _t          <- LeaseTransactionV3.create(None, _sender, _recipient, amount, fee, timestamp, atomicBadge, _proofs)
    } yield _t
}

object SignedLeaseV3Request {
  implicit val broadcastLeaseRequestReadsFormat: Format[SignedLeaseV3Request] = Json.format
}
