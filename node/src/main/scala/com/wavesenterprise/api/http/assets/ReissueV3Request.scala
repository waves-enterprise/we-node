package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.assets.ReissueTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class ReissueV3Request(version: Byte,
                            sender: String,
                            assetId: String,
                            quantity: Long,
                            reissuable: Boolean,
                            fee: Long,
                            atomicBadge: Option[AtomicBadge],
                            timestamp: Option[Long] = None,
                            password: Option[String] = None)
    extends UnsignedTxRequest {
  import com.wavesenterprise.api.http.assets.ReissueV3Request.reissueFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> ReissueTransaction.typeId)
  }
}

object ReissueV3Request {
  implicit val reissueFormat: Format[ReissueV3Request] = Json.format
}
