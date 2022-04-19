package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.{AssetIdStringLength, DataTransactionV2, Proofs, ValidationError}
import play.api.libs.json.{Json, Reads}

object DataRequestV2 {
  implicit val unsignedDataRequestV2Reads: Reads[DataRequestV2]     = Json.reads[DataRequestV2]
  implicit val signedDataRequestV2Reads: Reads[SignedDataRequestV2] = Json.reads[SignedDataRequestV2]
}

case class DataRequestV2(version: Byte,
                         sender: String,
                         senderPublicKey: Option[String],
                         author: String,
                         data: List[DataEntry[_]],
                         fee: Long,
                         feeAssetId: Option[String] = None,
                         timestamp: Option[Long] = None,
                         password: Option[String] = None)
    extends DataRequest

case class SignedDataRequestV2(version: Byte,
                               senderPublicKey: String,
                               authorPublicKey: String,
                               data: List[DataEntry[_]],
                               fee: Long,
                               timestamp: Long,
                               feeAssetId: Option[String],
                               proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, DataTransactionV2] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _author     <- PublicKeyAccount.fromBase58String(authorPublicKey).leftMap(ValidationError.fromCryptoError)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      _proofs     <- Proofs.create(_proofBytes)
      _           <- DataTransactionV2.ensureSupportedVersion(version)
      t           <- DataTransactionV2.create(_sender, _author, data, timestamp, fee, _feeAssetId, _proofs)
    } yield t
}
