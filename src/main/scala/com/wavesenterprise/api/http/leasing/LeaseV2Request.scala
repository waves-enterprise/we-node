package com.wavesenterprise.api.http.leasing

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.lease.LeaseTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class LeaseV2Request(version: Byte,
                          sender: String,
                          amount: Long,
                          fee: Long,
                          recipient: String,
                          timestamp: Option[Long] = None,
                          password: Option[String] = None)
    extends UnsignedTxRequest {
  import com.wavesenterprise.api.http.leasing.LeaseV2Request.leaseCancelRequestFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> LeaseTransaction.typeId)
  }
}

object LeaseV2Request {
  implicit val leaseCancelRequestFormat: Format[LeaseV2Request] = Json.format
}
