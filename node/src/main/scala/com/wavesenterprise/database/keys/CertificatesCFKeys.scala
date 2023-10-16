package com.wavesenterprise.database.keys

import com.google.common.primitives.Longs
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.database.KeyHelpers.{bytes, h}
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.CertsCF
import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import com.wavesenterprise.database.{MainDBKey, RocksDBSet, readCert, readSet, writeSet}
import com.wavesenterprise.database.RocksDBSet._
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.pki.CrlData
import monix.eval.Coeval
import org.apache.commons.codec.digest.DigestUtils

import java.net.URL
import java.security.cert.Certificate

object CertificatesCFKeys {
  private[this] val CertByDnHashPrefix: Short      = 1
  private[this] val CertDnHashByPublicKey: Short   = 2
  private[this] val CertDnHashByFingerprint: Short = 3
  private[this] val CertDnHashesAtHeight: Short    = 4

  private[this] val CrlIssuersPrefix: Short      = 5
  private[this] val CrlUrlsByIssuerPrefix: Short = 6
  private[database] val CrlByKeyPrefix: Short    = 7
  private[this] val CrlByHashPrefix: Short       = 8

  private[this] val Sha1Size = 20

  def certByDnHash(distinguishedNameHash: ByteStr): MainDBKey[Option[Certificate]] =
    MainDBKey.opt("cert-by-dn-hash", CertsCF, bytes(CertByDnHashPrefix, distinguishedNameHash.arr), readCert, _.getEncoded)

  def certDnHashByPublicKey(publicKey: PublicKeyAccount): MainDBKey[Option[ByteStr]] =
    MainDBKey.opt("cert-dn-hash-by-public-key", CertsCF, bytes(CertDnHashByPublicKey, publicKey.publicKey.getEncoded), ByteStr(_), _.arr)

  def certDnHashByFingerprint(fingerprint: ByteStr): MainDBKey[Option[ByteStr]] =
    MainDBKey.opt("cert-dn-hash-by-fingerprint", CertsCF, bytes(CertDnHashByFingerprint, fingerprint.arr), ByteStr(_), _.arr)

  def certDnHashesAtHeight(height: Int): MainDBKey[Set[ByteStr]] =
    MainDBKey("cert-dn-hashes-at-height", CertsCF, h(CertDnHashesAtHeight, height), readSet(ByteStr(_), Sha1Size), writeSet(_.arr, Sha1Size))

  def crlIssuers(storage: MainRocksDBStorage): MainRocksDBSet[PublicKeyAccount] = {
    RocksDBSet.newMain(
      name = "crl-issuers",
      columnFamily = CertsCF,
      storage = storage,
      prefix = bytes(CrlIssuersPrefix, Array.emptyByteArray),
      itemEncoder = _.publicKey.getEncoded,
      itemDecoder = PublicKeyAccount.apply
    )
  }

  def crlUrlsByIssuerPublicKey(publicKey: PublicKeyAccount, storage: MainRocksDBStorage): RocksDBSet.MainRocksDBSet[URL] = {
    RocksDBSet.newMain(
      name = "crl-urls-by-issuer-public-key",
      columnFamily = CertsCF,
      storage = storage,
      prefix = bytes(CrlUrlsByIssuerPrefix, publicKey.publicKey.getEncoded),
      itemEncoder = _.toString.getBytes,
      itemDecoder = bytes => new URL(new String(bytes))
    )
  }

  def crlDataByHash(hash: ByteStr): MainDBKey[Option[CrlData]] =
    MainDBKey.opt("crl-data-by-hash", CertsCF, bytes(CrlByHashPrefix, hash.arr), CrlData.fromBytesUnsafe(_), _.bytes())

  def crlHashByKey(crlKey: CrlKey): MainDBKey[Option[ByteStr]] =
    MainDBKey.opt("crl-hash-by-pubkey-cdpHash-timestamp", CertsCF, bytes(CrlByKeyPrefix, crlKey.bytes()), ByteStr(_), _.arr)

}

case class CrlKey(publicKeyAccount: PublicKeyAccount, cdp: URL, crlTimestamp: Long) {
  val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    Array.concat(publicKeyAccount.publicKey.getEncoded, DigestUtils.sha1(cdp.toString), Longs.toByteArray(crlTimestamp))
  }
}
