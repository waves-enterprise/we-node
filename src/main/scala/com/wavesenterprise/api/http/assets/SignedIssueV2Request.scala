package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.google.common.base.Charsets
import com.wavesenterprise.account.{AddressScheme, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.assets.IssueTransactionV2
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Reads}

object SignedIssueV2Request {
  implicit val signedExchangeRequestReads: Reads[SignedIssueV2Request] = {
    (
      (JsPath \ "version").read[Byte] and
        (JsPath \ "senderPublicKey").read[String] and
        (JsPath \ "name").read[String] and
        (JsPath \ "description").read[String] and
        (JsPath \ "quantity").read[Long] and
        (JsPath \ "decimals").read[Byte] and
        (JsPath \ "reissuable").read[Boolean] and
        (JsPath \ "fee").read[Long] and
        (JsPath \ "timestamp").read[Long] and
        (JsPath \ "proofs").read[List[ProofStr]] and
        (JsPath \ "script").readNullable[String]
    )(SignedIssueV2Request.apply _)
  }
}

case class SignedIssueV2Request(version: Byte,
                                senderPublicKey: String,
                                name: String,
                                description: String,
                                quantity: Long,
                                decimals: Byte,
                                reissuable: Boolean,
                                fee: Long,
                                timestamp: Long,
                                proofs: List[String],
                                script: Option[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, IssueTransactionV2] =
    for {
      _sender     <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _script <- script match {
        case None    => Right(None)
        case Some(s) => Script.fromBase64String(s).map(Some(_))
      }
      _ <- IssueTransactionV2.ensureSupportedVersion(version)
      t <- IssueTransactionV2.create(
        AddressScheme.getAddressSchema.chainId,
        _sender,
        name.getBytes(Charsets.UTF_8),
        description.getBytes(Charsets.UTF_8),
        quantity,
        decimals,
        reissuable,
        fee,
        timestamp,
        _script,
        _proofs
      )
    } yield t
}
