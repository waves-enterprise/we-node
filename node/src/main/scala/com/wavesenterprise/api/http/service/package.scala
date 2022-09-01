package com.wavesenterprise.api.http

import cats.implicits.{catsStdBitraverseForEither, toBifunctorOps}
import com.wavesenterprise.api.http.ApiError.OverflowError
import com.wavesenterprise.api.{decodeBase64Certificates, parseCertChain}
import com.wavesenterprise.certs.CertChain
import play.api.libs.json.JsObject

import scala.util.{Failure, Success, Try}

package object service {
  def validateSum(x: Int, y: Int): Either[ApiError, Int] = {
    Try(Math.addExact(x, y)) match {
      case Failure(_)     => Left(OverflowError)
      case Success(value) => Right(value)
    }
  }

  def certChainFromJson(jsv: JsObject): Either[ApiError, Option[CertChain]] = {
    (jsv \ "certificates").asOpt[List[String]].fold[Either[ApiError, Option[CertChain]]](Right(None)) { base64Certs =>
      if (base64Certs.isEmpty) {
        Right(None)
      } else {
        decodeBase64Certificates(base64Certs).flatMap(parseCertChain).leftMap(ApiError.fromValidationError)
      }
    }
  }
}
