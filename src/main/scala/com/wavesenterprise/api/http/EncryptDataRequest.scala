package com.wavesenterprise.api.http

import com.google.common.base.CaseFormat
import com.wavesenterprise.crypto
import com.wavesenterprise.protobuf.constants.CryptoAlgo
import com.wavesenterprise.protobuf.constants.CryptoAlgo.AES
import play.api.libs.functional.syntax.toFunctionalBuilderOps
import play.api.libs.json._

object JsonCryptoAlgo {
  private val jsonNameToValue: Map[String, CryptoAlgo] = CryptoAlgo.values.map { algo =>
    CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, algo.name) -> algo
  }.toMap

  val possibleValuesMessage: String =
    List(AES)
      .map(v => s"'${CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, v.name)}'")
      .mkString(",")

  def readsCryptoAlgo(str: String): Reads[CryptoAlgo] =
    if (str.isEmpty) {
      Reads(_ => JsError(s"'cryptoAlgo' parameter is required. Possible values: $possibleValuesMessage"))
    } else {
      jsonNameToValue
        .get(str.toLowerCase)
        .map(Reads.pure(_))
        .getOrElse(Reads(_ => JsError(s"Unknown crypto algorithm '$str'. Possible values: $possibleValuesMessage")))
    }

  implicit val writesCryptoAlgo: Writes[CryptoAlgo] = Writes { algo =>
    JsString(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, algo.name))
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
  import JsonCryptoAlgo.{readsCryptoAlgo, writesCryptoAlgo}

  implicit val writes: Writes[EncryptDataRequest] = Json.writes[EncryptDataRequest]

  implicit val reads: Reads[EncryptDataRequest] = (
    (JsPath \ "sender").read[String] and
      (JsPath \ "password").readNullable[String] and
      (JsPath \ "encryptionText").read[String] and
      (JsPath \ "recipientsPublicKeys").read[List[String]] and
      (JsPath \ "cryptoAlgo").readWithDefault[String]("").flatMap(readsCryptoAlgo)
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
  import JsonCryptoAlgo.{readsCryptoAlgo, writesCryptoAlgo}

  implicit val writes: Writes[DecryptDataRequest] = Json.writes[DecryptDataRequest]

  implicit val reads: Reads[DecryptDataRequest] = (
    (JsPath \ "recipient").read[String] and
      (JsPath \ "password").readNullable[String] and
      (JsPath \ "encryptedText").read[String] and
      (JsPath \ "wrappedKey").read[String] and
      (JsPath \ "senderPublicKey").read[String] and
      (JsPath \ "cryptoAlgo").readWithDefault[String]("").flatMap(readsCryptoAlgo)
  )(DecryptDataRequest.apply _)
}

case class DecryptDataResponse(
    decryptedText: String
)
object DecryptDataResponse {
  implicit val decryptDataResponseFormat: OFormat[DecryptDataResponse] = Json.format
}
