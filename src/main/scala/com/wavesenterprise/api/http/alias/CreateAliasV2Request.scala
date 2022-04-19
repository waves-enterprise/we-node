package com.wavesenterprise.api.http.alias

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.CreateAliasTransaction
import play.api.libs.json.{Format, JsObject, Json}

case class CreateAliasV2Request(version: Byte,
                                sender: String,
                                alias: String,
                                fee: Long,
                                timestamp: Option[Long] = None,
                                password: Option[String] = None)
    extends UnsignedTxRequest {

  import com.wavesenterprise.api.http.alias.CreateAliasV2Request.aliasV2RequestFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> CreateAliasTransaction.typeId)
  }
}

object CreateAliasV2Request {
  implicit val aliasV2RequestFormat: Format[CreateAliasV2Request] = Json.format
}
