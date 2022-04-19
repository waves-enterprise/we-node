package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.{CallContractTransaction, CallContractTransactionV4}
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[CallContractTransactionV4]] request
  */
case class SignedCallContractRequestV4(
    version: Int,
    senderPublicKey: String,
    contractId: String,
    contractVersion: Int,
    params: List[DataEntry[_]],
    fee: Long,
    feeAssetId: Option[String],
    timestamp: Long,
    atomicBadge: Option[AtomicBadge],
    proofs: List[String]
) extends BroadcastRequest {

  def toTx: Either[ValidationError, CallContractTransactionV4] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx         <- CallContractTransactionV4.create(sender, contractId, params, fee, timestamp, contractVersion, feeAssetId, atomicBadge, proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(CallContractTransaction.typeId.toInt))
}

object SignedCallContractRequestV4 {

  implicit val format: OFormat[SignedCallContractRequestV4] = Json.format

}
