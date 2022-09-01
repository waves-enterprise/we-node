package com.wavesenterprise

import com.wavesenterprise.account.{AddressOrAlias, PublicKeyAccount}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.Proofs
import com.wavesenterprise.utils.Base58
import org.scalatest.matchers.{HavePropertyMatchResult, HavePropertyMatcher}
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

package object http {

  def sameSignature(target: Array[Byte])(actual: Array[Byte]): Boolean = target sameElements actual

  implicit class JsFieldTypeChecker(val s: String) extends AnyVal {
    def ofType[A <: JsValue](implicit r: Reads[A], t: ClassTag[A]): HavePropertyMatcher[JsValue, Any] = HavePropertyMatcher[JsValue, Any] { json =>
      val actualValue = (json \ s).validate[A]
      HavePropertyMatchResult(actualValue.isSuccess, s, t.runtimeClass.getSimpleName, (json \ s).get)
    }
  }

  implicit def tuple2ToHPM(v: (String, JsValue)): HavePropertyMatcher[JsValue, JsValue] =
    HavePropertyMatcher[JsValue, JsValue] { json =>
      val actualFieldValue = (json \ v._1).as[JsValue]
      HavePropertyMatchResult(actualFieldValue == v._2, v._1, v._2, actualFieldValue)
    }

  implicit val byteStrFormat: Format[ByteStr] = Format(
    Reads {
      case JsString(str) =>
        ByteStr.decodeBase58(str) match {
          case Success(x) => JsSuccess(x)
          case Failure(e) => JsError(e.getMessage)
        }

      case _ => JsError("Can't read PublicKeyAccount")
    },
    Writes(x => JsString(x.base58))
  )

  implicit val publicKeyAccountFormat: Format[PublicKeyAccount] = byteStrFormat.inmap[PublicKeyAccount](
    x => PublicKeyAccount(x.arr),
    x => ByteStr(x.publicKey.getEncoded)
  )

  implicit val proofsFormat: Format[Proofs] = Format(
    Reads {
      case JsArray(xs) =>
        xs.foldLeft[JsResult[Proofs]](JsSuccess(Proofs.empty)) {
          case (r: JsError, _) => r
          case (JsSuccess(r, _), JsString(rawProof)) =>
            ByteStr.decodeBase58(rawProof) match {
              case Failure(e) => JsError(e.toString)
              case Success(x) => JsSuccess(Proofs(r.proofs :+ x))
            }
          case _ => JsError("Can't parse proofs")
        }
      case _ => JsError("Can't parse proofs")
    },
    Writes { proofs =>
      JsArray(proofs.proofs.map(byteStrFormat.writes))
    }
  )

  implicit val addressOrAliasFormat: Format[AddressOrAlias] = Format[AddressOrAlias](
    Reads {
      case JsString(str) =>
        Base58
          .decode(str)
          .toEither
          .flatMap(AddressOrAlias.fromBytes(_, 0))
          .map { case (x, _) => JsSuccess(x) }
          .getOrElse(JsError("Can't read PublicKeyAccount"))

      case _ => JsError("Can't read PublicKeyAccount")
    },
    Writes(x => JsString(x.bytes.base58))
  )
}
