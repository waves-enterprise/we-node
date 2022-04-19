package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.transaction._
import play.api.libs.json.{Json, OFormat}

case class SignedUpdatePolicyRequestV3(senderPublicKey: String,
                                       policyId: String,
                                       recipients: List[String],
                                       owners: List[String],
                                       opType: String,
                                       timestamp: Long,
                                       fee: Long,
                                       feeAssetId: Option[String],
                                       atomicBadge: Option[AtomicBadge],
                                       proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, UpdatePolicyTransactionV3] = {
    for {
      senderPublicKey  <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes       <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      parsedProofs     <- Proofs.create(proofBytes)
      parsedRecipients <- recipients.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- owners.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOpType     <- OpType.fromStr(opType)
      policyIdDecoded  <- PrivacyApiService.decodePolicyId(policyId)
      parsedFeeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid.feeAssetId", AssetIdStringLength)
      tx = UpdatePolicyTransactionV3(senderPublicKey,
                                     policyIdDecoded,
                                     parsedRecipients,
                                     parsedOwners,
                                     parsedOpType,
                                     timestamp,
                                     fee,
                                     parsedFeeAssetId,
                                     atomicBadge,
                                     parsedProofs)
    } yield tx
  }
}

object SignedUpdatePolicyRequestV3 {
  implicit val format: OFormat[SignedUpdatePolicyRequestV3] = Json.format
}
