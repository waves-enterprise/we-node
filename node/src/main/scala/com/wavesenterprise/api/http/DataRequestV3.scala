package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, DataTransactionV3, Proofs, ValidationError}
import play.api.libs.json.{Json, Reads}

object DataRequestV3 {
  implicit val unsignedDataRequestV3Reads: Reads[DataRequestV3]     = Json.reads[DataRequestV3]
  implicit val signedDataRequestV3Reads: Reads[SignedDataRequestV3] = Json.reads[SignedDataRequestV3]
}

case class DataRequestV3(version: Byte,
                         sender: String,
                         senderPublicKey: Option[String],
                         author: String,
                         data: List[DataEntry[_]],
                         fee: Long,
                         atomicBadge: Option[AtomicBadge],
                         feeAssetId: Option[String] = None,
                         timestamp: Option[Long] = None,
                         password: Option[String] = None)
    extends DataRequest

case class SignedDataRequestV3(version: Byte,
                               senderPublicKey: String,
                               authorPublicKey: String,
                               data: List[DataEntry[_]],
                               fee: Long,
                               timestamp: Long,
                               feeAssetId: Option[String],
                               atomicBadge: Option[AtomicBadge],
                               proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, DataTransactionV3] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _author     <- PublicKeyAccount.fromBase58String(authorPublicKey).leftMap(ValidationError.fromCryptoError)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      _proofs     <- Proofs.create(_proofBytes)
      _           <- DataTransactionV3.ensureSupportedVersion(version)
      t           <- DataTransactionV3.create(_sender, _author, data, timestamp, fee, _feeAssetId, atomicBadge, _proofs)
    } yield t
}
