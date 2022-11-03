package com.wavesenterprise.database.certs

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.pki.CrlData

import java.security.cert.{Certificate, X509Certificate}
import scala.annotation.tailrec

trait CertificatesState {

  def certByDistinguishedNameHash(dnHash: String): Option[Certificate]

  def certByDistinguishedName(distinguishedName: String): Option[Certificate]

  def certByPublicKey(publicKey: PublicKeyAccount): Option[Certificate]

  def certByFingerPrint(fingerprint: ByteStr): Option[Certificate]

  def certsAtHeight(height: Int): Set[Certificate]

  def certChainByPublicKey(publicKey: PublicKeyAccount): List[Certificate] = {
    buildCertChain(certByPublicKey(publicKey))
  }

  def crlDataByHash(crlHash: ByteStr): Option[CrlData]

  def actualCrls(issuer: PublicKeyAccount, timestamp: Long): Set[CrlData]

  @tailrec
  private def buildCertChain(maybeCert: Option[Certificate], acc: List[Certificate] = Nil): List[Certificate] = {
    maybeCert match {
      case None => acc
      case Some(cert) =>
        val issuerDn = cert.asInstanceOf[X509Certificate].getIssuerX500Principal
        if (issuerDn == cert.asInstanceOf[X509Certificate].getSubjectX500Principal) {
          cert :: acc
        } else {
          val issuerCert = certByDistinguishedName(issuerDn.getName)
          buildCertChain(issuerCert, cert :: acc)
        }
    }
  }
}
