package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.smart.SetScriptTransaction
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

case class SetScriptRequest(version: Byte,
                            sender: String,
                            script: Option[String],
                            name: String,
                            description: Option[String],
                            fee: Long,
                            timestamp: Option[Long] = None,
                            password: Option[String] = None)
    extends UnsignedTxRequest {}

object SetScriptRequest {
  implicit val jsonFormat: Format[SetScriptRequest] = Json.format
  implicit class SetScriptRequestExt(val self: SetScriptRequest) extends AnyVal {
    def toJsObject: JsObject = Json.toJson(self).as[JsObject] + ("type" -> JsNumber(SetScriptTransaction.typeId.toInt))
  }
}
