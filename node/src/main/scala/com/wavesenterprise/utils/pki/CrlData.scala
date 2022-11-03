package com.wavesenterprise.utils.pki

import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.PublicKey
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.serialization.BinarySerializer.Offset
import com.wavesenterprise.state.ByteStr
import monix.eval.Coeval
import org.apache.commons.codec.digest.DigestUtils

import java.net.URL
import java.security.cert.X509CRL
import scala.util.Try

case class CrlData(crl: X509CRL, issuer: PublicKeyAccount, cdp: URL) {
  val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    val out = newDataOutput()
    BinarySerializer.writeX509Crl(crl, out)
    val encoded = issuer.publicKey.getEncoded
    out.write(encoded)
    BinarySerializer.writeShortString(cdp.toString, out)
    out.toByteArray
  }

  val crlHash: Coeval[ByteStr] = Coeval.evalOnce {
    CrlData.hashForCrl(crl)
  }
}

object CrlData {
  def fromBytes(bytes: Array[Byte], offset: Int = 0): Try[CrlData] = {
    for {
      (crl, crlEnd) <- Try(BinarySerializer.parseX509Crl(bytes, offset))
      issuerEnd = crlEnd + crypto.KeyLength
      issuer      <- Try(PublicKeyAccount(PublicKey(bytes.slice(crlEnd, issuerEnd))))
      (urlStr, _) <- Try(BinarySerializer.parseShortString(bytes, issuerEnd))
      cdp         <- Try(new URL(urlStr))
    } yield CrlData(crl, issuer, cdp)
  }

  def fromBytesUnsafe(bytes: Array[Byte], offset: Int = 0): CrlData = {
    fromBytes(bytes, offset).get
  }

  def writeCrlData(value: CrlData, output: ByteArrayDataOutput): Unit = {
    output.write(value.bytes())
  }

  def parseCrlData(bytes: Array[Byte], offset: Offset = 0): (CrlData, Offset) =
    (
      for {
        (crl, crlEnd) <- Try(BinarySerializer.parseX509Crl(bytes, offset))
        issuerEnd = crlEnd + crypto.KeyLength
        issuer        <- Try(PublicKeyAccount(PublicKey(bytes.slice(crlEnd, issuerEnd))))
        (urlStr, end) <- Try(BinarySerializer.parseShortString(bytes, issuerEnd))
        cdp           <- Try(new URL(urlStr))
      } yield (CrlData(crl, issuer, cdp), end)
    ).get

  def hashForCrl(crl: X509CRL): ByteStr = ByteStr(DigestUtils.sha1(crl.getEncoded))
}
