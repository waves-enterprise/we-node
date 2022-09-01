package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.DisableContractTransactionV2
import com.wavesenterprise.transaction.{AssetIdStringLength, Proofs, ValidationError}
import play.api.libs.json.{Json, OFormat}

/**
  * Signed [[DisableContractTransactionV2]] request
  */
case class SignedDisableContractRequestV2(senderPublicKey: String,
                                          contractId: String,
                                          fee: Long,
                                          timestamp: Long,
                                          feeAssetId: Option[String],
                                          proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, DisableContractTransactionV2] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx         <- DisableContractTransactionV2.create(sender, contractId, fee, timestamp, feeAssetId, proofs)
    } yield tx
}

object SignedDisableContractRequestV2 {

  implicit val format: OFormat[SignedDisableContractRequestV2] = Json.format

}
