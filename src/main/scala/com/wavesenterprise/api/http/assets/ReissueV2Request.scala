package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.assets.ReissueTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class ReissueV2Request(version: Byte,
                            sender: String,
                            assetId: String,
                            quantity: Long,
                            reissuable: Boolean,
                            fee: Long,
                            timestamp: Option[Long] = None,
                            password: Option[String] = None)
    extends UnsignedTxRequest {
  import com.wavesenterprise.api.http.assets.ReissueV2Request.reissueFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> ReissueTransaction.typeId)
  }
}

object ReissueV2Request {
  implicit val reissueFormat: Format[ReissueV2Request] = Json.format
}
