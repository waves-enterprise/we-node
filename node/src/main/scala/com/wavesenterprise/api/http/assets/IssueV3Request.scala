package com.wavesenterprise.api.http.assets

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.assets.{IssueTransaction, IssueTransactionV3}
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

case class IssueV3Request(version: Byte,
                          sender: String,
                          name: String,
                          description: String,
                          quantity: Long,
                          decimals: Byte,
                          reissuable: Boolean,
                          script: Option[String],
                          fee: Long,
                          timestamp: Option[Long],
                          atomicBadge: Option[AtomicBadge],
                          password: Option[String] = None)
    extends UnsignedTxRequest {

  import com.wavesenterprise.api.http.assets.IssueV3Request.issueV3RequestFormat

  def mkJson: JsObject = {
    Json.toJson(this).as[JsObject] ++ Json.obj("type" -> IssueTransaction.typeId)
  }
}

object IssueV3Request {
  implicit val issueV3RequestFormat: Format[IssueV3Request] = Json.format

  implicit class SmartIssueRequestExt(val self: IssueV3Request) extends AnyVal {
    def toJsObject: JsObject = Json.toJson(self).as[JsObject] + ("type" -> JsNumber(IssueTransactionV3.typeId.toInt))
  }

}
