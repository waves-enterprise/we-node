package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import play.api.libs.json.{Format, JsNumber, JsObject, Json}
import com.wavesenterprise.transaction.assets.SetAssetScriptTransactionV1

case class SetAssetScriptRequest(version: Byte,
                                 sender: String,
                                 assetId: String,
                                 script: Option[String],
                                 fee: Long,
                                 timestamp: Option[Long] = None,
                                 password: Option[String] = None)
    extends UnsignedTxRequest

object SetAssetScriptRequest {
  implicit val jsonFormat: Format[SetAssetScriptRequest] = Json.format
  implicit class SetAssetScriptRequestExt(val self: SetAssetScriptRequest) extends AnyVal {
    def toJsObject: JsObject = Json.toJson(self).as[JsObject] + ("type" -> JsNumber(SetAssetScriptTransactionV1.typeId.toInt))
  }
}
