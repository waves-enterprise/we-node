package com.wavesenterprise.api.http.leasing

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.lease.LeaseCancelTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class LeaseCancelV2Request(version: Byte,
                                sender: String,
                                txId: String,
                                fee: Long,
                                timestamp: Option[Long] = None,
                                password: Option[String] = None)
    extends UnsignedTxRequest {

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> LeaseCancelTransaction.typeId)
  }
}

object LeaseCancelV2Request {
  implicit val leaseCancelRequestFormat: Format[LeaseCancelV2Request] = Json.format
}
