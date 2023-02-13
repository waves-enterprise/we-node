package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.validation.PolicyValidation
import com.wavesenterprise.transaction.{CreatePolicyTransactionV1, Proofs, ValidationError}
import play.api.libs.json.{Json, OFormat}

case class SignedCreatePolicyRequestV1(senderPublicKey: String,
                                       policyName: String,
                                       description: String,
                                       recipients: List[String],
                                       owners: List[String],
                                       timestamp: Long,
                                       fee: Long,
                                       proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, CreatePolicyTransactionV1] = {
    for {
      _                <- Either.cond(policyName.length <= PolicyValidation.MaxPolicyNameLength, (), GenericError("policy name is too long"))
      _                <- Either.cond(description.length < Short.MaxValue, (), GenericError("policy description is too long"))
      senderPublicKey  <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes       <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      parsedProofs     <- Proofs.create(proofBytes)
      parsedRecipients <- recipients.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- owners.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      tx = CreatePolicyTransactionV1(senderPublicKey, policyName, description, parsedRecipients, parsedOwners, timestamp, fee, parsedProofs)
    } yield tx
  }
}

object SignedCreatePolicyRequestV1 {
  implicit val format: OFormat[SignedCreatePolicyRequestV1] = Json.format
}
