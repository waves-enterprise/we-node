package com.wavesenterprise.api.http

import play.api.libs.json.{Format, JsPath, Json, Reads}
import play.api.libs.functional.syntax._

case class SignedMessage(message: String, signature: String, publickey: String)

object SignedMessage {

  implicit val messageReads: Reads[SignedMessage] = (
    (JsPath \ "message").read[String] and
      (JsPath \ "signature").read[String] and
      (JsPath \ "publickey")
        .read[String]
        .orElse((JsPath \ "publicKey").read[String])
  )(SignedMessage.apply _)

}

case class Message(message: String, password: Option[String])

object Message {
  implicit val format: Format[Message] = Json.format
}
