package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.validation.TransferValidation
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Reads}

object SignedMassTransferRequestV3 {
  implicit val MassTransferRequestReads: Reads[SignedMassTransferRequestV3] = (
    (JsPath \ "version").read[Byte] and
      (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "assetId").readNullable[String] and
      (JsPath \ "transfers").read[List[TransferDescriptor]] and
      (JsPath \ "fee").read[Long] and
      (JsPath \ "timestamp").read[Long] and
      (JsPath \ "attachment").readNullable[String] and
      (JsPath \ "feeAssetId").readNullable[String] and
      (JsPath \ "atomicBadge").readNullable[AtomicBadge] and
      (JsPath \ "proofs").read[List[ProofStr]]
  )(SignedMassTransferRequestV3.apply _)
}

case class SignedMassTransferRequestV3(version: Byte,
                                       senderPublicKey: String,
                                       assetId: Option[String],
                                       transfers: List[TransferDescriptor],
                                       fee: Long,
                                       timestamp: Long,
                                       attachment: Option[String],
                                       feeAssetId: Option[String],
                                       atomicBadge: Option[AtomicBadge],
                                       proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, MassTransferTransactionV3] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _assetId    <- parseBase58ToOption(assetId.filter(_.nonEmpty), "invalid.assetId", AssetIdStringLength)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _attachment <- parseBase58(attachment.filter(_.nonEmpty), "invalid.attachment", TransferValidation.MaxAttachmentStringSize)
      _feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid.feeAssetId", AssetIdStringLength)
      _transfers  <- MassTransferRequest.parseTransfersList(transfers)
      _           <- MassTransferTransactionV3.ensureSupportedVersion(version)
      t           <- MassTransferTransactionV3.create(_sender, _assetId, _transfers, timestamp, fee, _attachment.arr, _feeAssetId, atomicBadge, _proofs)
    } yield t
}
