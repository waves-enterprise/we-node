package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.validation.TransferValidation
import com.wavesenterprise.transaction.{AssetIdStringLength, Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json._

object SignedMassTransferRequestV1 {
  implicit val MassTransferRequestReads: Reads[SignedMassTransferRequestV1] = (
    (JsPath \ "version").read[Byte] and
      (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "assetId").readNullable[String] and
      (JsPath \ "transfers").read[List[TransferDescriptor]] and
      (JsPath \ "fee").read[Long] and
      (JsPath \ "timestamp").read[Long] and
      (JsPath \ "attachment").readNullable[String] and
      (JsPath \ "proofs").read[List[ProofStr]]
  )(SignedMassTransferRequestV1.apply _)
}

case class SignedMassTransferRequestV1(version: Byte,
                                       senderPublicKey: String,
                                       assetId: Option[String],
                                       transfers: List[TransferDescriptor],
                                       fee: Long,
                                       timestamp: Long,
                                       attachment: Option[String],
                                       proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, MassTransferTransactionV1] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _assetId    <- parseBase58ToOption(assetId.filter(_.nonEmpty), "invalid.assetId", AssetIdStringLength)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _attachment <- parseBase58(attachment.filter(_.nonEmpty), "invalid.attachment", TransferValidation.MaxAttachmentStringSize)
      _transfers  <- MassTransferRequest.parseTransfersList(transfers)
      _           <- MassTransferTransactionV1.ensureSupportedVersion(version)
      t           <- MassTransferTransactionV1.create(_sender, _assetId, _transfers, timestamp, fee, _attachment.arr, _proofs)
    } yield t
}
