package com.wavesenterprise.network.peers

import cats.data.Validated
import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto.PublicKey
import io.netty.buffer.{ByteBuf, ByteBufUtil}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.security.cert.{CertificateFactory, X509Certificate}

sealed trait PeerIdentityResponse {

  def bytes: Array[Byte]

  def encode(out: ByteBuf): ByteBuf
}

final case class SuccessPeerIdentityResponse(pk: PublicKey, certificates: List[X509Certificate]) extends PeerIdentityResponse {

  lazy val bytes: Array[Byte] = {
    val (encodedLength, encodedCerts) = certificates.foldRight((0, List.empty[Array[Byte]])) {
      case (cert, (lenAcc, certAcc)) =>
        val encoded = cert.getEncoded
        (lenAcc + encoded.length) -> (encoded :: certAcc)
    }
    val byteBuffer = java.nio.ByteBuffer.allocate(
      java.lang.Byte.BYTES + java.lang.Integer.BYTES + pk.getEncoded.length + java.lang.Integer.BYTES + encodedLength + encodedCerts.size * java.lang.Integer.BYTES)
    byteBuffer.put(PeerIdentityResponse.SuccessByte)
    byteBuffer.putInt(pk.getEncoded.length)
    byteBuffer.put(pk.getEncoded)
    byteBuffer.putInt(encodedCerts.size)
    encodedCerts.foreach { encodedCert =>
      byteBuffer.putInt(encodedCert.length)
      byteBuffer.put(encodedCert)
    }
    byteBuffer.array()
  }

  def encode(out: ByteBuf): ByteBuf = {
    out.writeBytes(bytes)
    out
  }

  override lazy val toString: String = {
    s"SuccessPeerIdentityResponse(${PublicKeyAccount(pk)})"
  }
}

final case class MessagePeerIdentityResponse(message: String) extends PeerIdentityResponse {

  lazy val bytes: Array[Byte] = {
    val messageBytes = message.getBytes(StandardCharsets.UTF_8)
    val byteBuffer   = java.nio.ByteBuffer.allocate(java.lang.Byte.BYTES + java.lang.Integer.BYTES + messageBytes.length)
    byteBuffer.put(PeerIdentityResponse.MessageByte)
    byteBuffer.putInt(messageBytes.length)
    byteBuffer.put(messageBytes)
    byteBuffer.array()
  }

  def encode(out: ByteBuf): ByteBuf = {
    out.writeBytes(bytes)
    out
  }

  override lazy val toString: String = {
    s"MessagePeerIdentityResponse($message)"
  }
}

object PeerIdentityResponse {

  val SuccessByte: Byte = 1
  val MessageByte: Byte = 2

  def decode(in: ByteBuf): Either[Throwable, PeerIdentityResponse] = Either.catchNonFatal {
    val responseType = in.readByte()
    responseType match {
      case SuccessByte =>
        val pkSize     = in.readInt()
        val pkBytes    = ByteBufUtil.getBytes(in.readBytes(pkSize))
        val certsCount = Validated.catchOnly[IndexOutOfBoundsException](in.readInt()).getOrElse(0)
        val certificates = (0 until certsCount).map { _ =>
          val certLength = in.readInt()
          val certBytes  = ByteBufUtil.getBytes(in.readBytes(certLength))
          val factory    = CertificateFactory.getInstance("X.509")
          val cert       = factory.generateCertificate(new ByteArrayInputStream(certBytes))
          cert.asInstanceOf[X509Certificate]
        }

        SuccessPeerIdentityResponse(PublicKey(pkBytes), certificates.toList)
      case MessageByte =>
        val messageSize = in.readInt()
        MessagePeerIdentityResponse(new String(ByteBufUtil.getBytes(in.readBytes(messageSize)), StandardCharsets.UTF_8))
      case byte =>
        throw new RuntimeException(
          s"Unknown message byte '$byte' for PeerIdentityResponse. Expected bytes are '$SuccessByte' (success), '$MessageByte' (message)")
    }
  }
}
