package com.wavesenterprise

import cats.implicits._
import com.wavesenterprise.certs.{CertChain, CertChainStore}
import com.wavesenterprise.serialization.BinarySerializer.x509CertFromBytes
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.utils.{Base64, ScorexLogging}

import scala.util.Try

package object api extends ScorexLogging {
  def parseCertChain(certificatesBytes: List[Array[Byte]]): Either[ValidationError, Option[CertChain]] = {
    if (certificatesBytes.isEmpty) {
      Right(None)
    } else {
      (for {
        certificates <- certificatesBytes.traverse { certBytes =>
          Try(x509CertFromBytes(certBytes)).toEither.leftMap(_ =>
            ValidationError.CertificateParseError("Expected a valid X.509 certificate in DER encoding"))
        }
        certChainStore <- CertChainStore.fromCertificates(certificates).leftMap(ValidationError.fromCryptoError)
        certChains     <- certChainStore.getCertChains.leftMap(ValidationError.fromCryptoError)
        _ <- Either.cond(
          certChains.size == 1,
          (),
          ValidationError.CertificateParseError(s"Unexpected cert chains number. Provided: '${certChains.size}', expected: '1'")
        )
      } yield certChains.headOption).left.map { validationError =>
        log.error(s"Unable to parse cert chain from the request: $validationError")
        validationError
      }
    }
  }

  def decodeBase64Certificates(certificatesBase64: List[String]): Either[ValidationError, List[Array[Byte]]] = {
    certificatesBase64.traverse {
      Base64
        .decode(_)
        .toEither
        .leftMap(err => ValidationError.CertificateParseError(s"Encountered an invalid Base64: ${err.getMessage}"))
    }
  }
}
