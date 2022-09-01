package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.CallContractTransactionV1
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.json.{Json, OFormat}

/**
  * Signed [[CallContractTransactionV1]] request
  */
case class SignedCallContractRequestV1(senderPublicKey: String,
                                       contractId: String,
                                       params: List[DataEntry[_]],
                                       fee: Long,
                                       timestamp: Long,
                                       proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, CallContractTransactionV1] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      tx         <- CallContractTransactionV1.create(sender, contractId, params, fee, timestamp, proofs)
    } yield tx
}

object SignedCallContractRequestV1 {

  implicit val format: OFormat[SignedCallContractRequestV1] = Json.format

}
