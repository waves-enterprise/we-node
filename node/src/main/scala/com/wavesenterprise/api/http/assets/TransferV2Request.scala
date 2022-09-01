package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.transaction.transfer.TransferTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class TransferV2Request(
    version: Byte,
    assetId: Option[String],
    amount: Long,
    feeAssetId: Option[String],
    fee: Long,
    sender: String,
    attachment: Option[String],
    recipient: String,
    timestamp: Option[Long] = None,
    password: Option[String] = None
) extends UnsignedTxRequest
    with SponsoredFeesSupport {

  import com.wavesenterprise.api.http.assets.TransferV2Request.transferV2RequestFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> TransferTransaction.typeId)
  }
}

object TransferV2Request {
  implicit val transferV2RequestFormat: Format[TransferV2Request] = Json.format
}
