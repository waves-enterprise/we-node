package com.wavesenterprise.api.http.alias

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.CreateAliasTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class CreateAliasV3Request(version: Byte,
                                sender: String,
                                alias: String,
                                fee: Long,
                                feeAssetId: Option[String] = None,
                                timestamp: Option[Long] = None,
                                password: Option[String] = None)
    extends UnsignedTxRequest {

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> CreateAliasTransaction.typeId)
  }
}

object CreateAliasV3Request {
  implicit val aliasV3RequestFormat: Format[CreateAliasV3Request] = Json.format
}
