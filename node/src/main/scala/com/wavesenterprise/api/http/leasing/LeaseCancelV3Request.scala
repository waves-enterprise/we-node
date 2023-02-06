package com.wavesenterprise.api.http.leasing

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.lease.LeaseCancelTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class LeaseCancelV3Request(version: Byte,
                                sender: String,
                                txId: String,
                                fee: Long,
                                atomicBadge: Option[AtomicBadge],
                                timestamp: Option[Long] = None,
                                password: Option[String] = None)
    extends UnsignedTxRequest {

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> LeaseCancelTransaction.typeId)
  }
}

object LeaseCancelV3Request {
  implicit val leaseCancelRequestFormat: Format[LeaseCancelV3Request] = Json.format
}
