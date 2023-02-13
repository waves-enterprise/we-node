package com.wavesenterprise.api.http.alias

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.{AtomicBadge, CreateAliasTransaction}
import play.api.libs.json.{Format, JsObject, Json}

case class CreateAliasV4Request(version: Byte,
                                sender: String,
                                alias: String,
                                fee: Long,
                                atomicBadge: Option[AtomicBadge],
                                feeAssetId: Option[String] = None,
                                timestamp: Option[Long] = None,
                                password: Option[String] = None)
    extends UnsignedTxRequest {

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> CreateAliasTransaction.typeId)
  }
}

object CreateAliasV4Request {
  implicit val aliasV4RequestFormat: Format[CreateAliasV4Request] = Json.format
}
