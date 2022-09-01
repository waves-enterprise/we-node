package com.wavesenterprise.api.http

import cats.implicits._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{Proofs, UpdatePolicyTransactionV1, ValidationError}
import com.wavesenterprise.utils.Base58
import play.api.libs.json.{Json, OFormat}

case class SignedUpdatePolicyRequestV1(senderPublicKey: String,
                                       policyId: String,
                                       recipients: List[String],
                                       owners: List[String],
                                       opType: String,
                                       timestamp: Long,
                                       fee: Long,
                                       proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, UpdatePolicyTransactionV1] = {
    for {
      senderPublicKey  <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes       <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      parsedProofs     <- Proofs.create(proofBytes)
      parsedRecipients <- recipients.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- owners.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOpType     <- OpType.fromStr(opType)
      policyIdDecoded  <- Base58.decode(policyId).toEither.leftMap(_ => GenericError(s"Failed to decode policyId ($policyId) to Base58"))
      tx = UpdatePolicyTransactionV1(senderPublicKey,
                                     ByteStr(policyIdDecoded),
                                     parsedRecipients,
                                     parsedOwners,
                                     parsedOpType,
                                     timestamp,
                                     fee,
                                     parsedProofs)
    } yield tx
  }
}

object SignedUpdatePolicyRequestV1 {
  implicit val format: OFormat[SignedUpdatePolicyRequestV1] = Json.format
}
