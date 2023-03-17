package com.wavesenterprise.api.http.privacy

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.UpdatePolicyTransactionV2
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

case class UpdatePolicyRequestV2(sender: String,
                                 policyId: String,
                                 recipients: Set[String],
                                 owners: Set[String],
                                 opType: String,
                                 timestamp: Option[Long],
                                 fee: Long,
                                 feeAssetId: Option[String],
                                 password: Option[String])
    extends UnsignedTxRequest {

  def toJson: JsObject =
    Json.toJson(this).as[JsObject] +
      ("type"    -> JsNumber(UpdatePolicyTransactionV2.typeId)) +
      ("version" -> JsNumber(2))
}

object UpdatePolicyRequestV2 {
  implicit val format: Format[UpdatePolicyRequestV2] = Json.format
}
