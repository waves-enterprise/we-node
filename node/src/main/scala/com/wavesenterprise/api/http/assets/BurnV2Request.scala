package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.assets.BurnTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class BurnV2Request(version: Byte,
                         sender: String,
                         assetId: String,
                         quantity: Long,
                         fee: Long,
                         timestamp: Option[Long] = None,
                         password: Option[String] = None)
    extends UnsignedTxRequest {
  import com.wavesenterprise.api.http.assets.BurnV2Request.burnV2Format

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> BurnTransaction.typeId)
  }
}

object BurnV2Request {
  implicit val burnV2Format: Format[BurnV2Request] = Json.format
}
