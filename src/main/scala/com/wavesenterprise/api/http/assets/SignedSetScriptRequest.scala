package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.google.common.base.Charsets
import com.wavesenterprise.account.{AddressScheme, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.smart.{SetScriptTransaction, SetScriptTransactionV1}
import com.wavesenterprise.transaction.{Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json._

object SignedSetScriptRequest {
  implicit val signedSetScriptRequestReads: Reads[SignedSetScriptRequest] = (
    (JsPath \ "version").read[Byte] and
      (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "script").readNullable[String] and
      (JsPath \ "name").read[String] and
      (JsPath \ "description").readNullable[String] and
      (JsPath \ "fee").read[Long] and
      (JsPath \ "timestamp").read[Long] and
      (JsPath \ "proofs").read[List[ProofStr]]
  )(SignedSetScriptRequest.apply _)

  implicit val signedSetScriptRequestWrites: OWrites[SignedSetScriptRequest] = Json.writes[SignedSetScriptRequest]
}

case class SignedSetScriptRequest(version: Byte,
                                  senderPublicKey: String,
                                  script: Option[String],
                                  name: String,
                                  description: Option[String],
                                  fee: Long,
                                  timestamp: Long,
                                  proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, SetScriptTransaction] =
    for {
      _sender <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _script <- script match {
        case Some(s) if s.nonEmpty => Script.fromBase64String(s).map(Some(_))
        case _                     => Right(None)
      }
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      _           <- SetScriptTransactionV1.ensureSupportedVersion(version)
      t <- SetScriptTransactionV1.create(
        AddressScheme.getAddressSchema.chainId,
        _sender,
        _script,
        name.getBytes(Charsets.UTF_8),
        description.getOrElse("").getBytes(Charsets.UTF_8),
        fee,
        timestamp,
        _proofs
      )
    } yield t
}
