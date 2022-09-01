package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.wavesenterprise.account.{AddressScheme, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.assets.BurnTransactionV2
import com.wavesenterprise.transaction.{AssetIdStringLength, Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json, Reads, Writes}

case class SignedBurnV2Request(version: Byte,
                               senderPublicKey: String,
                               assetId: String,
                               quantity: Long,
                               fee: Long,
                               timestamp: Long,
                               proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, BurnTransactionV2] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _assetId    <- parseBase58(assetId, "invalid.assetId", AssetIdStringLength)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      chainId = AddressScheme.getAddressSchema.chainId
      _  <- BurnTransactionV2.ensureSupportedVersion(version)
      _t <- BurnTransactionV2.create(chainId, _sender, _assetId, quantity, fee, timestamp, _proofs)
    } yield _t
}

object SignedBurnV2Request {
  implicit val reads: Reads[SignedBurnV2Request] = (
    (JsPath \ "version").read[Byte] and
      (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "assetId").read[String] and
      (JsPath \ "quantity").read[Long].orElse((JsPath \ "amount").read[Long]) and
      (JsPath \ "fee").read[Long] and
      (JsPath \ "timestamp").read[Long] and
      (JsPath \ "proofs").read[List[ProofStr]]
  )(SignedBurnV2Request.apply _)

  implicit val writes: Writes[SignedBurnV2Request] = Json.writes[SignedBurnV2Request]
}
