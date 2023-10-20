package com.wavesenterprise.api.http.leasing

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.lease.LeaseTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class LeaseV3Request(version: Byte,
                          sender: String,
                          amount: Long,
                          fee: Long,
                          recipient: String,
                          atomicBadge: Option[AtomicBadge],
                          timestamp: Option[Long] = None,
                          password: Option[String] = None)
    extends UnsignedTxRequest {
  import com.wavesenterprise.api.http.leasing.LeaseV3Request.leaseCancelRequestFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> LeaseTransaction.typeId)
  }
}

object LeaseV3Request {
  implicit val leaseCancelRequestFormat: Format[LeaseV3Request] = Json.format
}
