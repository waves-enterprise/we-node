package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.wavesenterprise.account.{AddressOrAlias, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.validation.TransferValidation
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{Json, OFormat}

case class SignedTransferV3Request(senderPublicKey: String,
                                   assetId: Option[String],
                                   recipient: String,
                                   amount: Long,
                                   feeAssetId: Option[String],
                                   fee: Long,
                                   timestamp: Long,
                                   version: Byte,
                                   attachment: Option[String],
                                   proofs: List[String],
                                   atomicBadge: Option[AtomicBadge])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, TransferTransactionV3] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _assetId    <- parseBase58ToOption(assetId.filter(_.nonEmpty), "invalid.assetId", AssetIdStringLength)
      _feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid.assetId", AssetIdStringLength)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _recipient  <- AddressOrAlias.fromString(recipient).leftMap(ValidationError.fromCryptoError)
      _attachment <- parseBase58(attachment.filter(_.nonEmpty), "invalid.attachment", TransferValidation.MaxAttachmentStringSize)
      _           <- TransferTransactionV3.ensureSupportedVersion(version)
      t           <- TransferTransactionV3.create(_sender, _assetId, _feeAssetId, timestamp, amount, fee, _recipient, _attachment.arr, atomicBadge, _proofs)
    } yield t
}

object SignedTransferV3Request {
  implicit val format: OFormat[SignedTransferV3Request] = Json.format
}
