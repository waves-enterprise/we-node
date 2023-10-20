package com.wavesenterprise.api.http.privacy

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.{AtomicBadge, UpdatePolicyTransaction}
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

case class UpdatePolicyRequestV3(
    sender: String,
    policyId: String,
    recipients: Set[String],
    owners: Set[String],
    opType: String,
    timestamp: Option[Long],
    fee: Long,
    feeAssetId: Option[String],
    atomicBadge: Option[AtomicBadge],
    password: Option[String]
) extends UnsignedTxRequest {

  def toJson: JsObject =
    Json.toJson(this).as[JsObject] +
      ("type"    -> JsNumber(UpdatePolicyTransaction.typeId.toInt)) +
      ("version" -> JsNumber(3))
}

object UpdatePolicyRequestV3 {
  implicit val format: Format[UpdatePolicyRequestV3] = Json.format
}
