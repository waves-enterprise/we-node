package com.wavesenterprise.api.http.alias

import cats.implicits._
import com.wavesenterprise.account.{Alias, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.{CreateAliasTransaction, CreateAliasTransactionV2, Proofs, ValidationError}
import play.api.libs.json.{Format, Json}

case class SignedCreateAliasV2Request(version: Byte, senderPublicKey: String, fee: Long, alias: String, timestamp: Long, proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, CreateAliasTransaction] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _alias      <- Alias.buildWithCurrentChainId(alias).leftMap(ValidationError.fromCryptoError)
      _           <- CreateAliasTransactionV2.ensureSupportedVersion(version)
      _t          <- CreateAliasTransactionV2.create(_sender, _alias, fee, timestamp, _proofs)
    } yield _t
}

object SignedCreateAliasV2Request {
  implicit val broadcastAliasV2RequestReadsFormat: Format[SignedCreateAliasV2Request] = Json.format
}
