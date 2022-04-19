package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.assets.{IssueTransaction, IssueTransactionV2}
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

case class IssueV2Request(version: Byte,
                          sender: String,
                          name: String,
                          description: String,
                          quantity: Long,
                          decimals: Byte,
                          reissuable: Boolean,
                          script: Option[String],
                          fee: Long,
                          timestamp: Option[Long],
                          password: Option[String] = None)
    extends UnsignedTxRequest {

  import com.wavesenterprise.api.http.assets.IssueV2Request.issueV2RequestFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> IssueTransaction.typeId)
  }
}

object IssueV2Request {
  implicit val issueV2RequestFormat: Format[IssueV2Request] = Json.format

  implicit class SmartIssueRequestExt(val self: IssueV2Request) extends AnyVal {
    def toJsObject: JsObject = Json.toJson(self).as[JsObject] + ("type" -> JsNumber(IssueTransactionV2.typeId.toInt))
  }

}
