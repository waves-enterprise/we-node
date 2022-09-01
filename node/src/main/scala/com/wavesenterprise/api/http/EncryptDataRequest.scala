package com.wavesenterprise.api.http

import com.google.common.base.CaseFormat
import play.api.libs.functional.syntax.toFunctionalBuilderOps
import play.api.libs.json._

trait JsonCryptoAlgo {
  def cryptoAlgos: Set[String]

  lazy val possibleValuesMessage: String = cryptoAlgos.mkString(",")

  implicit val cryptoAlgoReads: Reads[CryptoAlgo] = {
    case JsString(value) =>
      if (value.isEmpty) {
        JsError(s"'cryptoAlgo' parameter is required. Possible values: $possibleValuesMessage")
      } else {
        if (cryptoAlgos.contains(value)) {
          JsSuccess(CryptoAlgo(value))
        } else {
          JsError(s"Unknown crypto algorithm '$value'. Possible values: $possibleValuesMessage")
        }
      }
    case _ => JsError(s"Invalid 'cryptoAlgo' parameter. Expected string value.")
  }
}

class CryptoAlgo(val value: String) extends AnyVal

object CryptoAlgo {
  def apply(str: String) = new CryptoAlgo(str)

  implicit val writesCryptoAlgo: Writes[CryptoAlgo] = Writes { algo =>
    val lowerHyphenValue = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, algo.value)
    JsString(lowerHyphenValue)
  }
}

case class EncryptDataRequest(
    sender: String,
    password: Option[String],
    encryptionText: String,
    recipientsPublicKeys: List[String],
    cryptoAlgo: CryptoAlgo
)

object EncryptDataRequest {
  implicit val writes: Writes[EncryptDataRequest] = Json.writes[EncryptDataRequest]

  implicit def reads(implicit cryptoAlgoReads: Reads[CryptoAlgo]): Reads[EncryptDataRequest] =
    (
      (JsPath \ "sender").read[String] and
        (JsPath \ "password").readNullable[String] and
        (JsPath \ "encryptionText").read[String] and
        (JsPath \ "recipientsPublicKeys").read[List[String]] and
        (JsPath \ "cryptoAlgo").read[CryptoAlgo]
    )(EncryptDataRequest.apply _)
}

case class DecryptDataRequest(
    recipient: String,
    password: Option[String],
    encryptedText: String,
    wrappedKey: String,
    senderPublicKey: String,
    cryptoAlgo: CryptoAlgo
)
object DecryptDataRequest {
  implicit val writes: Writes[DecryptDataRequest] = Json.writes[DecryptDataRequest]

  implicit def reads(implicit cryptoAlgoReads: Reads[CryptoAlgo]): Reads[DecryptDataRequest] =
    (
      (JsPath \ "recipient").read[String] and
        (JsPath \ "password").readNullable[String] and
        (JsPath \ "encryptedText").read[String] and
        (JsPath \ "wrappedKey").read[String] and
        (JsPath \ "senderPublicKey").read[String] and
        (JsPath \ "cryptoAlgo").read[CryptoAlgo]
    )(DecryptDataRequest.apply _)
}

case class DecryptDataResponse(
    decryptedText: String
)

object DecryptDataResponse {
  implicit val decryptDataResponseFormat: OFormat[DecryptDataResponse] = Json.format
}
