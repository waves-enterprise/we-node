package com.wavesenterprise.api.http.docker

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.ValidationError.InvalidContractId
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import com.wavesenterprise.transaction.docker.{CallContractTransaction, CallContractTransactionV7}
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, Proofs, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

/**
  * Signed [[CallContractTransactionV7]] request
  */
case class SignedCallContractRequestV7(version: Int,
                                       senderPublicKey: String,
                                       contractId: String,
                                       contractVersion: Int,
                                       params: List[DataEntry[_]],
                                       payments: List[ContractTransferInV1],
                                       fee: Long,
                                       feeAssetId: Option[String],
                                       timestamp: Long,
                                       atomicBadge: Option[AtomicBadge],
                                       inputCommitment: Option[String],
                                       contractEngine: String,
                                       callFunc: Option[String],
                                       proofs: List[String])
    extends BroadcastRequest {

  def toTx: Either[ValidationError, CallContractTransactionV7] =
    for {
      sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      feeAssetId <- parseBase58ToOption(feeAssetId.filter(_.nonEmpty), "invalid feeAssetId", AssetIdStringLength)
      proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      proofs     <- Proofs.create(proofBytes)
      contractId <- ByteStr.decodeBase58(contractId).fold(_ => Left(InvalidContractId(contractId)), Right(_))
      commitment <- inputCommitment.traverse((Commitment.fromBase58 _).andThen(_.leftMap(ValidationError.fromCryptoError)))
      tx <- CallContractTransactionV7.create(sender,
                                             contractId,
                                             params,
                                             fee,
                                             timestamp,
                                             contractVersion,
                                             feeAssetId,
                                             atomicBadge,
                                             payments,
                                             commitment,
                                             contractEngine,
                                             callFunc,
                                             proofs)
    } yield tx

  def toJson: JsObject = Json.toJsObject(this) + ("type" -> JsNumber(CallContractTransaction.typeId.toInt))
}

object SignedCallContractRequestV7 {

  implicit val format: OFormat[SignedCallContractRequestV7] = Json.format

}
