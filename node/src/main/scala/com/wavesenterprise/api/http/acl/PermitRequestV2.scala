package com.wavesenterprise.api.http.acl

import com.wavesenterprise.acl.{OpType, PermissionOp, Role}
import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.transaction.acl.PermitTransaction
import com.wavesenterprise.transaction.{AtomicBadge, ValidationError}
import play.api.libs.json._

case class PermitRequestV2(sender: String,
                           target: String,
                           role: String,
                           opType: String,
                           fee: Long,
                           timestamp: Option[Long],
                           dueTimestamp: Option[Long],
                           atomicBadge: Option[AtomicBadge],
                           password: Option[String] = None)
    extends UnsignedTxRequest {

  def mkPermissionOp(ts: Long): Either[ValidationError, PermissionOp] =
    for {
      r  <- Role.fromStr(role)
      op <- OpType.fromStr(opType)
    } yield PermissionOp(op, r, ts, dueTimestamp)

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(PermitTransaction.typeId.toInt))
}

object PermitRequestV2 {
  implicit val format: Format[PermitRequestV2] = Json.format
}
