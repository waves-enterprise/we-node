package com.wavesenterprise.api.http

import com.wavesenterprise.transaction.RegisterNodeTransactionV1
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

case class RegisterNodeRequest(sender: String,
                               targetPubKey: String,
                               nodeName: Option[String],
                               opType: String,
                               timestamp: Option[Long],
                               fee: Long,
                               password: Option[String] = None)
    extends UnsignedTxRequest {

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(RegisterNodeTransactionV1.typeId.toInt))
}

object RegisterNodeRequest {
  implicit val format: Format[RegisterNodeRequest] = Json.format
}
