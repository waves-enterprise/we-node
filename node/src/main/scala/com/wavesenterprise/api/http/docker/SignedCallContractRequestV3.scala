package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.CallContractTransactionV3
import com.wavesenterprise.transaction.{AssetIdStringLength, Proofs, ValidationError}
import play.api.libs.json.{Json, OFormat}

/**
  * Signed [[CallContractTransactionV3]] request
  */
case class SignedCallContractRequestV3(
    version: Int,
    senderPublicKey: String,
    contractId: String,
    contractVersion: Int,
    params: List[DataEntry[_]],
    fee: Long,
    feeAssetId: Option[String],
    timestamp: Long,
    proofs: List[String]
) extends BroadcastRequest {

  def toTx: Either[ValidationError, CallContractTransactionV3] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx         <- CallContractTransactionV3.create(sender, contractId, params, fee, timestamp, contractVersion, feeAssetId, proofs)
    } yield tx
}

object SignedCallContractRequestV3 {

  implicit val format: OFormat[SignedCallContractRequestV3] = Json.format

}
