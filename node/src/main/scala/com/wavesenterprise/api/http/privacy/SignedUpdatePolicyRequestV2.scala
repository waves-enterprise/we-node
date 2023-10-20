package com.wavesenterprise.api.http.privacy

import cats.implicits._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.transaction.{AssetIdStringLength, Proofs, UpdatePolicyTransactionV2, ValidationError}
import play.api.libs.json.{Json, OFormat}

case class SignedUpdatePolicyRequestV2(senderPublicKey: String,
                                       policyId: String,
                                       recipients: List[String],
                                       owners: List[String],
                                       opType: String,
                                       timestamp: Long,
                                       fee: Long,
                                       feeAssetId: Option[String],
                                       proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, UpdatePolicyTransactionV2] = {
    for {
      senderPublicKey  <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes       <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      parsedProofs     <- Proofs.create(proofBytes)
      parsedRecipients <- recipients.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- owners.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOpType     <- OpType.fromStr(opType)
      policyIdDecoded  <- PrivacyApiService.decodePolicyId(policyId)
      parsedFeeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid.feeAssetId", AssetIdStringLength)
      tx = UpdatePolicyTransactionV2(senderPublicKey,
                                     policyIdDecoded,
                                     parsedRecipients,
                                     parsedOwners,
                                     parsedOpType,
                                     timestamp,
                                     fee,
                                     parsedFeeAssetId,
                                     parsedProofs)
    } yield tx
  }
}

object SignedUpdatePolicyRequestV2 {
  implicit val format: OFormat[SignedUpdatePolicyRequestV2] = Json.format
}
