package com.wavesenterprise.api.http.privacy

import cats.implicits._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.validation.PolicyValidation
import play.api.libs.json.{Json, OFormat}

case class SignedCreatePolicyRequestV3(senderPublicKey: String,
                                       policyName: String,
                                       description: String,
                                       recipients: List[String],
                                       owners: List[String],
                                       timestamp: Long,
                                       fee: Long,
                                       feeAssetId: Option[String],
                                       atomicBadge: Option[AtomicBadge],
                                       proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, CreatePolicyTransactionV3] = {
    for {
      _                <- Either.cond(policyName.length <= PolicyValidation.MaxPolicyNameLength, (), GenericError("policy name is too long"))
      _                <- Either.cond(description.length < Short.MaxValue, (), GenericError("policy description is too long"))
      senderPublicKey  <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes       <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      parsedProofs     <- Proofs.create(proofBytes)
      parsedRecipients <- recipients.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- owners.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedFeeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid.feeAssetId", AssetIdStringLength)
      tx = CreatePolicyTransactionV3(senderPublicKey,
                                     policyName,
                                     description,
                                     parsedRecipients,
                                     parsedOwners,
                                     timestamp,
                                     fee,
                                     parsedFeeAssetId,
                                     atomicBadge,
                                     parsedProofs)
    } yield tx
  }
}

object SignedCreatePolicyRequestV3 {
  implicit val format: OFormat[SignedCreatePolicyRequestV3] = Json.format
}
