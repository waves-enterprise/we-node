package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.wavesenterprise.account.{AddressScheme, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.assets.ReissueTransactionV3
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Reads}

case class SignedReissueV3Request(senderPublicKey: String,
                                  version: Byte,
                                  assetId: String,
                                  quantity: Long,
                                  reissuable: Boolean,
                                  fee: Long,
                                  timestamp: Long,
                                  atomicBadge: Option[AtomicBadge],
                                  proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, ReissueTransactionV3] =
    for {
      _sender <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      chainId = AddressScheme.getAddressSchema.chainId
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _assetId    <- parseBase58(assetId, "invalid.assetId", AssetIdStringLength)
      _           <- ReissueTransactionV3.ensureSupportedVersion(version)
      _t          <- ReissueTransactionV3.create(chainId, _sender, _assetId, quantity, reissuable, fee, timestamp, atomicBadge, _proofs)
    } yield _t
}

object SignedReissueV3Request {
  implicit val assetReissueRequestReads: Reads[SignedReissueV3Request] = (
    (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "version").read[Byte] and
      (JsPath \ "assetId").read[String] and
      (JsPath \ "quantity").read[Long] and
      (JsPath \ "reissuable").read[Boolean] and
      (JsPath \ "fee").read[Long] and
      (JsPath \ "timestamp").read[Long] and
      (JsPath \ "atomicBadge").readNullable[AtomicBadge] and
      (JsPath \ "proofs").read[List[ProofStr]]
  )(SignedReissueV3Request.apply _)
}
