package com.wavesenterprise.matcher.api

import com.google.common.primitives.Longs
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.Base58
import monix.eval.Coeval
import play.api.libs.json._

case class CancelOrderRequest(sender: PublicKeyAccount, orderId: Option[ByteStr], timestamp: Option[Long], signature: Array[Byte]) {

  lazy val toSign: Array[Byte] = (orderId, timestamp) match {
    case (Some(oid), _)   => sender.publicKey.getEncoded ++ oid.arr
    case (None, Some(ts)) => sender.publicKey.getEncoded ++ Longs.toByteArray(ts)
    case (None, None)     => signature // Signature can't sign itself
  }

  val isSignatureValid: Coeval[Boolean] = Coeval.evalOnce(crypto.verify(signature, toSign, sender.publicKey))

  def json: JsObject = Json.obj(
    "sender"    -> sender.publicKeyBase58,
    "orderId"   -> orderId.map(_.base58),
    "signature" -> Base58.encode(signature),
    "timestamp" -> timestamp
  )
}

object CancelOrderRequest {
  implicit val byteArrayFormat: Format[Array[Byte]] = Format(
    {
      case JsString(base58String) => Base58.decode(base58String).fold(_ => JsError("Invalid signature"), b => JsSuccess(b))
      case other                  => JsError(s"Expecting string but got $other")
    },
    b => JsString(Base58.encode(b))
  )

  implicit val pkFormat: Format[PublicKeyAccount] = Format(
    {
      case JsString(value) => PublicKeyAccount.fromBase58String(value).fold(_ => JsError("Invalid public key"), pk => JsSuccess(pk))
      case other           => JsError(s"Expecting string but got $other")
    },
    pk => JsString(pk.publicKeyBase58)
  )

  implicit val format: OFormat[CancelOrderRequest] = Json.format
}
