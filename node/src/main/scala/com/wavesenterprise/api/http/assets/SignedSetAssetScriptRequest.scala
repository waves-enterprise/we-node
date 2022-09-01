package com.wavesenterprise.api.http.assets

import cats.implicits._
import com.wavesenterprise.account.{AddressScheme, PublicKeyAccount}
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.transaction.assets.SetAssetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.{AssetIdStringLength, Proofs, ValidationError}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Reads}

object SignedSetAssetScriptRequest {
  implicit val signedSetAssetScriptRequestReads: Reads[SignedSetAssetScriptRequest] = (
    (JsPath \ "version").read[Byte] and
      (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "assetId").read[String] and
      (JsPath \ "script").readNullable[String] and
      (JsPath \ "fee").read[Long] and
      (JsPath \ "timestamp").read[Long] and
      (JsPath \ "proofs").read[List[ProofStr]]
  )(SignedSetAssetScriptRequest.apply _)

}

case class SignedSetAssetScriptRequest(version: Byte,
                                       senderPublicKey: String,
                                       assetId: String,
                                       script: Option[String],
                                       fee: Long,
                                       timestamp: Long,
                                       proofs: List[String])
    extends BroadcastRequest {
  def toTx: Either[ValidationError, SetAssetScriptTransactionV1] =
    for {
      _sender  <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _assetId <- parseBase58(assetId, "invalid.assetId", AssetIdStringLength)
      _script <- script match {
        case Some(s) if s.nonEmpty => Script.fromBase64String(s).map(Some(_))
        case _                     => Right(None)
      }
      _proofBytes <- proofs.traverse(s => parseBase58(s, "invalid proof", Proofs.MaxProofStringSize))
      _proofs     <- Proofs.create(_proofBytes)
      chainId = AddressScheme.getAddressSchema.chainId
      _ <- SetAssetScriptTransactionV1.ensureSupportedVersion(version)
      t <- SetAssetScriptTransactionV1.create(chainId, _sender, _assetId, _script, fee, timestamp, _proofs)
    } yield t
}
