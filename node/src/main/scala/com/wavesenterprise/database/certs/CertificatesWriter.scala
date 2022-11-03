package com.wavesenterprise.database.certs

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.database.rocksdb.{RW, ReadWriteDB}
import com.wavesenterprise.state.ByteStr

import java.net.URL
import java.security.cert.{X509CRL, X509Certificate}

trait CertificatesWriter extends CertificatesState with ReadWriteDB {

  protected[database] def putCert(rw: RW, cert: X509Certificate): Unit

  protected[database] def putCertsAtHeight(rw: RW, height: Int, certs: Set[X509Certificate]): Unit

  protected[database] def putCrl(rw: RW, publicKeyAccount: PublicKeyAccount, cdp: URL, crl: X509CRL, crlHash: ByteStr): Unit

  def putCert(cert: X509Certificate): Unit = readWrite { rw =>
    putCert(rw, cert)
  }

  def putCertsAtHeight(height: Int, certs: Set[X509Certificate]): Unit = readWrite { rw =>
    putCertsAtHeight(rw, height, certs)
  }

  def putCrl(publicKeyAccount: PublicKeyAccount, cdp: URL, crl: X509CRL, crlHash: ByteStr): Unit = readWrite { rw =>
    putCrl(rw, publicKeyAccount, cdp, crl, crlHash)
  }

  def putCrlIssuers(issuers: Set[PublicKeyAccount]): Unit

  def putCrlUrlsForIssuerPublicKey(publicKey: PublicKeyAccount, crlUrls: Set[URL]): Unit
}
