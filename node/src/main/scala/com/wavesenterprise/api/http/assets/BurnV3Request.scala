package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.assets.BurnTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class BurnV3Request(version: Byte,
                         sender: String,
                         assetId: String,
                         quantity: Long,
                         fee: Long,
                         atomicBadge: Option[AtomicBadge],
                         timestamp: Option[Long] = None,
                         password: Option[String] = None)
    extends UnsignedTxRequest {
  import com.wavesenterprise.api.http.assets.BurnV3Request.burnV3Format

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> BurnTransaction.typeId)
  }
}

object BurnV3Request {
  implicit val burnV3Format: Format[BurnV3Request] = Json.format
}
