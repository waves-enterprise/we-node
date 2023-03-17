package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.wavesenterprise.account.{AddressScheme, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.assets.BurnTransactionV3
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json, Reads, Writes}

case class SignedBurnV3Request(version: Byte,
                               senderPublicKey: String,
                               assetId: String,
                               quantity: Long,
                               fee: Long,
                               timestamp: Long,
                               atomicBadge: Option[AtomicBadge],
                               proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, BurnTransactionV3] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _assetId    <- parseBase58(assetId, "invalid.assetId", AssetIdStringLength)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      chainId = AddressScheme.getAddressSchema.chainId
      _  <- BurnTransactionV3.ensureSupportedVersion(version)
      _t <- BurnTransactionV3.create(chainId, _sender, _assetId, quantity, fee, timestamp, atomicBadge, _proofs)
    } yield _t
}

object SignedBurnV3Request {
  implicit val reads: Reads[SignedBurnV3Request] = (
    (JsPath \ "version").read[Byte] and
      (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "assetId").read[String] and
      (JsPath \ "quantity").read[Long].orElse((JsPath \ "amount").read[Long]) and
      (JsPath \ "fee").read[Long] and
      (JsPath \ "timestamp").read[Long] and
      (JsPath \ "atomicBadge").readNullable[AtomicBadge] and
      (JsPath \ "proofs").read[List[ProofStr]]
  )(SignedBurnV3Request.apply _)

  implicit val writes: Writes[SignedBurnV3Request] = Json.writes[SignedBurnV3Request]
}
