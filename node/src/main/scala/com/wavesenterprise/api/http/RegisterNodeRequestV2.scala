package com.wavesenterprise.api.http

import com.wavesenterprise.transaction.{AtomicBadge, RegisterNodeTransactionV2}
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

case class RegisterNodeRequestV2(sender: String,
                                 targetPubKey: String,
                                 nodeName: Option[String],
                                 opType: String,
                                 timestamp: Option[Long],
                                 fee: Long,
                                 password: Option[String] = None,
                                 atomicBadge: Option[AtomicBadge])
    extends UnsignedTxRequest {

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(RegisterNodeTransactionV2.typeId.toInt))
}

object RegisterNodeRequestV2 {
  implicit val format: Format[RegisterNodeRequestV2] = Json.format
}
