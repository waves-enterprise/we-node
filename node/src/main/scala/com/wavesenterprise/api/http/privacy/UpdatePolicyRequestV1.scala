package com.wavesenterprise.api.http.privacy

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.UpdatePolicyTransactionV1
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

case class UpdatePolicyRequestV1(sender: String,
                                 policyId: String,
                                 recipients: Set[String],
                                 owners: Set[String],
                                 opType: String,
                                 timestamp: Option[Long],
                                 fee: Long,
                                 password: Option[String])
    extends UnsignedTxRequest {
  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(UpdatePolicyTransactionV1.typeId.toInt))
}

object UpdatePolicyRequestV1 {
  implicit val format: Format[UpdatePolicyRequestV1] = Json.format
}
