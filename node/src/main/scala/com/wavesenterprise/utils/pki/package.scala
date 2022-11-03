package com.wavesenterprise.utils

import java.io.ByteArrayInputStream
import java.security.cert.{CertificateFactory, X509CRL, X509Certificate}
import scala.util.Try

package object pki {
  def x509CertFromBase64(base64Cert: String): Try[X509Certificate] =
    Base64.decode(base64Cert).map { derEncodedCert =>
      val cf   = CertificateFactory.getInstance("X.509")
      val cert = cf.generateCertificate(new ByteArrayInputStream(derEncodedCert))

      cert.asInstanceOf[X509Certificate]
    }

  def x509CrlFromBase64(base64Crl: String): Try[X509CRL] =
    Base64.decode(base64Crl).map { encodedCrl =>
      val cf  = CertificateFactory.getInstance("X.509")
      val crl = cf.generateCRL(new ByteArrayInputStream(encodedCrl))

      crl.asInstanceOf[X509CRL]
    }
}
