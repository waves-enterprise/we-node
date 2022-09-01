package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.transaction._
import play.api.libs.json.{Json, OFormat}

case class SignedPolicyDataHashRequestV3(senderPublicKey: String,
                                         dataHash: String,
                                         policyId: String,
                                         timestamp: Long,
                                         fee: Long,
                                         feeAssetId: Option[String],
                                         atomicBadge: Option[AtomicBadge],
                                         proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, PolicyDataHashTransaction] = {
    for {
      senderPublicKey  <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes       <- proofs.traverse(s => parseBase58(s, "Invalid proof", Proofs.MaxProofStringSize))
      parsedProofs     <- Proofs.create(proofBytes)
      parsedDataHash   <- PolicyDataHash.fromBase58String(dataHash)
      parsedPolicyId   <- PrivacyApiService.decodePolicyId(policyId)
      parsedFeeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "Invalid feeAssetId", AssetIdStringLength)
      tx <- PolicyDataHashTransactionV3.create(senderPublicKey,
                                               parsedDataHash,
                                               parsedPolicyId,
                                               timestamp,
                                               fee,
                                               parsedFeeAssetId,
                                               atomicBadge,
                                               parsedProofs)
    } yield tx
  }
}

object SignedPolicyDataHashRequestV3 {
  implicit val format: OFormat[SignedPolicyDataHashRequestV3] = Json.format
}
