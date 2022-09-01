package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.{DataTransactionV1, Proofs, ValidationError}
import play.api.libs.json.{Json, Reads}

object DataRequestV1 {
  implicit val unsignedDataRequestV1Reads: Reads[DataRequestV1]     = Json.reads[DataRequestV1]
  implicit val signedDataRequestV1Reads: Reads[SignedDataRequestV1] = Json.reads[SignedDataRequestV1]
}

case class DataRequestV1(version: Byte,
                         sender: String,
                         senderPublicKey: Option[String],
                         author: String,
                         data: List[DataEntry[_]],
                         fee: Long,
                         timestamp: Option[Long] = None,
                         password: Option[String] = None)
    extends DataRequest

case class SignedDataRequestV1(version: Byte,
                               senderPublicKey: String,
                               authorPublicKey: String,
                               data: List[DataEntry[_]],
                               fee: Long,
                               timestamp: Long,
                               proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, DataTransactionV1] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _author     <- PublicKeyAccount.fromBase58String(authorPublicKey).leftMap(ValidationError.fromCryptoError)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _           <- DataTransactionV1.ensureSupportedVersion(version)
      t           <- DataTransactionV1.create(_sender, _author, data, timestamp, fee, _proofs)
    } yield t
}
