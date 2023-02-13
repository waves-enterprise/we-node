package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.transfer.{MassTransferTransactionV2, MassTransferTransactionV3, TransferDescriptor}
import play.api.libs.json.{Format, JsObject, Json}

case class MassTransferRequestV3(version: Byte,
                                 assetId: Option[String],
                                 sender: String,
                                 transfers: List[TransferDescriptor],
                                 fee: Long,
                                 attachment: Option[String],
                                 atomicBadge: Option[AtomicBadge],
                                 timestamp: Option[Long] = None,
                                 feeAssetId: Option[String] = None,
                                 password: Option[String] = None)
    extends UnsignedTxRequest {

  import com.wavesenterprise.api.http.assets.MassTransferRequestV3.massTransferFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> MassTransferTransactionV3.typeId)
  }
}

object MassTransferRequestV3 {
  implicit val massTransferFormat: Format[MassTransferRequestV3] = Json.format[MassTransferRequestV3]
}
