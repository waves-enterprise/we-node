package com.wavesenterprise.api.http.privacy

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.CreatePolicyTransaction
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

case class CreatePolicyRequestV2(sender: String,
                                 policyName: String,
                                 description: String,
                                 recipients: Set[String],
                                 owners: Set[String],
                                 timestamp: Option[Long],
                                 fee: Long,
                                 feeAssetId: Option[String],
                                 password: Option[String])
    extends UnsignedTxRequest {

  def toJson: JsObject =
    Json.toJson(this).as[JsObject] +
      ("type"    -> JsNumber(CreatePolicyTransaction.typeId)) +
      ("version" -> JsNumber(2))
}

object CreatePolicyRequestV2 {
  implicit val format: OFormat[CreatePolicyRequestV2] = Json.format
}
