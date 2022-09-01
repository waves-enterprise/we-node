package com.wavesenterprise.database.certs

import com.wavesenterprise.database.rocksdb.{RW, ReadWriteDB}

import java.security.cert.X509Certificate

trait CertificatesWriter extends CertificatesState with ReadWriteDB {

  protected[database] def putCert(rw: RW, cert: X509Certificate): Unit

  protected[database] def putCertsAtHeight(rw: RW, height: Int, certs: Set[X509Certificate]): Unit

  def putCert(cert: X509Certificate): Unit = readWrite { rw =>
    putCert(rw, cert)
  }

  def putCertsAtHeight(height: Int, certs: Set[X509Certificate]): Unit = readWrite { rw =>
    putCertsAtHeight(rw, height, certs)
  }
}
