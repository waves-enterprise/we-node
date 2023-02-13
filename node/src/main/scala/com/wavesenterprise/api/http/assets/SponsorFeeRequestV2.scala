package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.{BroadcastRequest, UnsignedTxRequest}
import com.wavesenterprise.transaction.assets.{SponsorFeeTransactionV1, SponsorFeeTransactionV2}
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{JsObject, Json, OFormat}

case class SponsorFeeRequestV2(version: Byte,
                               sender: String,
                               assetId: String,
                               isEnabled: Boolean,
                               fee: Long,
                               atomicBadge: Option[AtomicBadge],
                               timestamp: Option[Long] = None,
                               password: Option[String] = None)
    extends UnsignedTxRequest {
  import com.wavesenterprise.api.http.assets.SponsorFeeRequestV2._

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> SponsorFeeTransactionV1.typeId)
  }
}

object SponsorFeeRequestV2 {
  implicit val unsignedSponsorRequestFormat: OFormat[SponsorFeeRequestV2] = Json.format[SponsorFeeRequestV2]
}

case class SignedSponsorFeeRequestV2(version: Byte,
                                     senderPublicKey: String,
                                     assetId: String,
                                     isEnabled: Boolean,
                                     fee: Long,
                                     timestamp: Long,
                                     atomicBadge: Option[AtomicBadge],
                                     proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, SponsorFeeTransactionV2] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _assetId    <- parseBase58(assetId, "invalid.assetId", AssetIdStringLength)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _           <- SponsorFeeTransactionV2.ensureSupportedVersion(version)
      t           <- SponsorFeeTransactionV2.create(_sender, _assetId, isEnabled, fee, timestamp, atomicBadge, _proofs)
    } yield t
}

object SignedSponsorFeeRequestV2 {
  implicit val signedSponsorRequestFormat: OFormat[SignedSponsorFeeRequestV2] = Json.format[SignedSponsorFeeRequestV2]
}
