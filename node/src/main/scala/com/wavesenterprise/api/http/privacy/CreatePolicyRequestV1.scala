package com.wavesenterprise.api.http.privacy

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.CreatePolicyTransaction
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

case class CreatePolicyRequestV1(sender: String,
                                 policyName: String,
                                 description: String,
                                 recipients: Set[String],
                                 owners: Set[String],
                                 timestamp: Option[Long],
                                 fee: Long,
                                 password: Option[String])
    extends UnsignedTxRequest {
  def toJson: JsObject =
    Json.toJson(this).as[JsObject] +
      ("type"    -> JsNumber(CreatePolicyTransaction.typeId.toInt)) +
      ("version" -> JsNumber(1))
}

object CreatePolicyRequestV1 {
  implicit val format: OFormat[CreatePolicyRequestV1] = Json.format
}
