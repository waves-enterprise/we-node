package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.transfer.TransferTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class TransferV3Request(
    version: Byte,
    assetId: Option[String],
    amount: Long,
    feeAssetId: Option[String],
    fee: Long,
    sender: String,
    attachment: Option[String],
    recipient: String,
    timestamp: Option[Long] = None,
    atomicBadge: Option[AtomicBadge],
    password: Option[String] = None
) extends UnsignedTxRequest
    with SponsoredFeesSupport {

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> TransferTransaction.typeId)
  }
}

object TransferV3Request {
  implicit val transferV3RequestFormat: Format[TransferV3Request] = Json.format
}
