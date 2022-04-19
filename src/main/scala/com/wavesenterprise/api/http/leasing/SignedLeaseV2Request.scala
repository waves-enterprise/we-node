package com.wavesenterprise.api.http.leasing

import cats.implicits._
import com.wavesenterprise.account.{AddressOrAlias, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.lease.LeaseTransactionV2
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.json.{Format, Json}

case class SignedLeaseV2Request(version: Byte,
                                senderPublicKey: String,
                                amount: Long,
                                fee: Long,
                                recipient: String,
                                timestamp: Long,
                                proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, LeaseTransactionV2] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _recipient  <- AddressOrAlias.fromString(recipient).leftMap(ValidationError.fromCryptoError)
      _           <- LeaseTransactionV2.ensureSupportedVersion(version)
      _t          <- LeaseTransactionV2.create(None, _sender, _recipient, amount, fee, timestamp, _proofs)
    } yield _t
}

object SignedLeaseV2Request {
  implicit val broadcastLeaseRequestReadsFormat: Format[SignedLeaseV2Request] = Json.format
}
