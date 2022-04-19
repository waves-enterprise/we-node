package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.transfer.{MassTransferTransactionV1, TransferDescriptor}
import play.api.libs.json.{Format, JsObject, Json}

case class MassTransferRequestV1(version: Byte,
                                 assetId: Option[String],
                                 sender: String,
                                 transfers: List[TransferDescriptor],
                                 fee: Long,
                                 attachment: Option[String],
                                 timestamp: Option[Long] = None,
                                 password: Option[String] = None)
    extends UnsignedTxRequest {

  import com.wavesenterprise.api.http.assets.MassTransferRequestV1.massTransferFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> MassTransferTransactionV1.typeId)
  }
}

object MassTransferRequestV1 {
  implicit val massTransferFormat: Format[MassTransferRequestV1] = Json.format[MassTransferRequestV1]
}
