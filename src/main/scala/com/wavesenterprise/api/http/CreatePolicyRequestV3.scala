package com.wavesenterprise.api.http

import com.wavesenterprise.transaction.{AtomicBadge, CreatePolicyTransaction}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

case class CreatePolicyRequestV3(
    sender: String,
    policyName: String,
    description: String,
    recipients: Set[String],
    owners: Set[String],
    timestamp: Option[Long],
    fee: Long,
    feeAssetId: Option[String],
    atomicBadge: Option[AtomicBadge],
    password: Option[String]
) extends UnsignedTxRequest {

  def toJson: JsObject =
    Json.toJson(this).as[JsObject] +
      ("type"    -> JsNumber(CreatePolicyTransaction.typeId.toInt)) +
      ("version" -> JsNumber(3))
}

object CreatePolicyRequestV3 {
  implicit val format: OFormat[CreatePolicyRequestV3] = Json.format
}
