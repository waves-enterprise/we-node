package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.{BroadcastRequest, UnsignedTxRequest}
import com.wavesenterprise.transaction.assets.SponsorFeeTransactionV1
import com.wavesenterprise.transaction.{AssetIdStringLength, Proofs, ValidationError}
import play.api.libs.json.{JsObject, Json, OFormat}

object SponsorFeeRequest {
  implicit val unsignedSponsorRequestFormat: OFormat[SponsorFeeRequest]     = Json.format[SponsorFeeRequest]
  implicit val signedSponsorRequestFormat: OFormat[SignedSponsorFeeRequest] = Json.format[SignedSponsorFeeRequest]
}

case class SponsorFeeRequest(version: Byte,
                             sender: String,
                             assetId: String,
                             isEnabled: Boolean,
                             fee: Long,
                             timestamp: Option[Long] = None,
                             password: Option[String] = None)
    extends UnsignedTxRequest {
  import com.wavesenterprise.api.http.assets.SponsorFeeRequest.unsignedSponsorRequestFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> SponsorFeeTransactionV1.typeId)
  }
}

case class SignedSponsorFeeRequest(version: Byte,
                                   senderPublicKey: String,
                                   assetId: String,
                                   isEnabled: Boolean,
                                   fee: Long,
                                   timestamp: Long,
                                   proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, SponsorFeeTransactionV1] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _assetId    <- parseBase58(assetId, "invalid.assetId", AssetIdStringLength)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _           <- SponsorFeeTransactionV1.ensureSupportedVersion(version)
      t           <- SponsorFeeTransactionV1.create(_sender, _assetId, isEnabled, fee, timestamp, _proofs)
    } yield t
}
