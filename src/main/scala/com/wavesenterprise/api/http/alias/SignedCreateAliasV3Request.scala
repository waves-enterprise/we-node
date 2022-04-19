package com.wavesenterprise.api.http.alias

import cats.implicits._
import com.wavesenterprise.account.{Alias, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction._
import play.api.libs.json.{Format, Json}

case class SignedCreateAliasV3Request(version: Byte,
                                      senderPublicKey: String,
                                      fee: Long,
                                      alias: String,
                                      timestamp: Long,
                                      feeAssetId: Option[String],
                                      proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, CreateAliasTransaction] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _alias      <- Alias.buildWithCurrentChainId(alias).leftMap(ValidationError.fromCryptoError)
      _feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid.feeAssetId", AssetIdStringLength)
      _           <- CreateAliasTransactionV3.ensureSupportedVersion(version)
      _t          <- CreateAliasTransactionV3.create(_sender, _alias, fee, timestamp, _feeAssetId, _proofs)
    } yield _t
}

object SignedCreateAliasV3Request {
  implicit val broadcastAliasV3RequestReadsFormat: Format[SignedCreateAliasV3Request] = Json.format
}
