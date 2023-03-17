package com.wavesenterprise.api.http.privacy

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.{AtomicBadge, PolicyDataHashTransaction}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

case class PolicyDataHashRequestV3(sender: String,
                                   dataHash: String,
                                   policyId: String,
                                   timestamp: Option[Long],
                                   fee: Long,
                                   feeAssetId: Option[String],
                                   atomicBadge: Option[AtomicBadge],
                                   password: Option[String])
    extends UnsignedTxRequest {

  def toJson: JsObject =
    Json.toJson(this).as[JsObject] +
      ("type"    -> JsNumber(PolicyDataHashTransaction.typeId)) +
      ("version" -> JsNumber(3))
}

object PolicyDataHashRequestV3 {
  implicit val format: OFormat[PolicyDataHashRequestV3] = Json.format
}
