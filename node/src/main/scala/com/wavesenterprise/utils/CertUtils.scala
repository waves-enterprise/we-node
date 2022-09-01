package com.wavesenterprise.utils

import java.io.ByteArrayInputStream
import java.security.cert.{CertificateFactory, X509Certificate}
import scala.util.Try

object CertUtils {

  def x509CertFromBase64(base64Cert: String): Try[X509Certificate] =
    Base64.decode(base64Cert).map { derEncodedCert =>
      val cf   = CertificateFactory.getInstance("X.509")
      val cert = cf.generateCertificate(new ByteArrayInputStream(derEncodedCert))

      cert.asInstanceOf[X509Certificate]
    }
}
