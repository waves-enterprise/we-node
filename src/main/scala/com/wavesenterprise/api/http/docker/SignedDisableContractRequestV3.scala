package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.DisableContractTransactionV3
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{Json, OFormat}

/**
  * Signed [[DisableContractTransactionV3]] request
  */
case class SignedDisableContractRequestV3(senderPublicKey: String,
                                          contractId: String,
                                          fee: Long,
                                          timestamp: Long,
                                          feeAssetId: Option[String],
                                          atomicBadge: Option[AtomicBadge],
                                          proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, DisableContractTransactionV3] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx         <- DisableContractTransactionV3.create(sender, contractId, fee, timestamp, feeAssetId, atomicBadge, proofs)
    } yield tx
}

object SignedDisableContractRequestV3 {

  implicit val format: OFormat[SignedDisableContractRequestV3] = Json.format

}
