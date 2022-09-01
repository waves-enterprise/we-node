package com.wavesenterprise.api.http

import com.wavesenterprise.transaction.AtomicTransaction
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

case class AtomicTransactionRequestV1(
    sender: String,
    transactions: List[JsObject],
    timestamp: Option[Long],
    password: Option[String] = None
) extends UnsignedTxRequest {

  def toJson: JsObject =
    Json.toJsObject(this) +
      ("type"    -> JsNumber(AtomicTransaction.typeId)) +
      ("version" -> JsNumber(1))
}

object AtomicTransactionRequestV1 {
  implicit val format: OFormat[AtomicTransactionRequestV1] = Json.format
}
