package com.wavesenterprise.database.keys

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.database.KeyHelpers.{bytes, h}
import com.wavesenterprise.database.rocksdb.ColumnFamily.CertsCF
import com.wavesenterprise.database.{Key, readCert, readSet, writeSet}
import com.wavesenterprise.state.ByteStr

import java.security.cert.Certificate

object CertificatesCFKeys {
  private[this] val CertByDnHashPrefix: Short      = 1
  private[this] val CertDnHashByPublicKey: Short   = 2
  private[this] val CertDnHashByFingerprint: Short = 3
  private[this] val CertDnHashesAtHeight: Short    = 4

  private[this] val Sha1Size = 20

  def certByDnHash(distinguishedNameHash: ByteStr): Key[Option[Certificate]] =
    Key.opt("cert-by-dn-hash", CertsCF, bytes(CertByDnHashPrefix, distinguishedNameHash.arr), readCert, _.getEncoded)

  def certDnHashByPublicKey(publicKey: PublicKeyAccount): Key[Option[ByteStr]] =
    Key.opt("cert-dn-hash-by-public-key", CertsCF, bytes(CertDnHashByPublicKey, publicKey.publicKey.getEncoded), ByteStr(_), _.arr)

  def certDnHashByFingerprint(fingerprint: ByteStr): Key[Option[ByteStr]] =
    Key.opt("cert-dn-hash-by-fingerprint", CertsCF, bytes(CertDnHashByFingerprint, fingerprint.arr), ByteStr(_), _.arr)

  def certDnHashesAtHeight(height: Int): Key[Set[ByteStr]] =
    Key("cert-dn-hashes-at-height", CertsCF, h(CertDnHashesAtHeight, height), readSet(ByteStr(_), Sha1Size), writeSet(_.arr, Sha1Size))
}
