package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.transfer.{MassTransferTransactionV2, TransferDescriptor}
import play.api.libs.json.{Format, JsObject, Json}

case class MassTransferRequestV2(version: Byte,
                                 assetId: Option[String],
                                 sender: String,
                                 transfers: List[TransferDescriptor],
                                 fee: Long,
                                 attachment: Option[String],
                                 timestamp: Option[Long] = None,
                                 feeAssetId: Option[String] = None,
                                 password: Option[String] = None)
    extends UnsignedTxRequest {

  import com.wavesenterprise.api.http.assets.MassTransferRequestV2.massTransferFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> MassTransferTransactionV2.typeId)
  }
}

object MassTransferRequestV2 {
  implicit val massTransferFormat: Format[MassTransferRequestV2] = Json.format[MassTransferRequestV2]
}
