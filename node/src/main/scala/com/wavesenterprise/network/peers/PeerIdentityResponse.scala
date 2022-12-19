package com.wavesenterprise.network.peers

import cats.implicits._
import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto.PublicKey
import com.wavesenterprise.serialization.BinarySerializer
import io.netty.buffer.{ByteBuf, ByteBufUtil}

import java.nio.charset.StandardCharsets
import java.security.cert.X509Certificate

sealed trait PeerIdentityResponse {

  def bytes: Array[Byte]

  def encode(out: ByteBuf): ByteBuf
}

final case class SuccessPeerIdentityResponse(pk: PublicKey, certificates: List[X509Certificate]) extends PeerIdentityResponse {

  // noinspection UnstableApiUsage
  lazy val bytes: Array[Byte] = {
    val output = newDataOutput()
    output.writeByte(PeerIdentityResponse.SuccessByte)
    BinarySerializer.writeBigByteArray(pk.getEncoded, output)
    BinarySerializer.writeShortIterable(certificates, BinarySerializer.writeX509Cert, output)
    output.toByteArray
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

  def decode(in: ByteBuf, decodeCerts: Boolean): Either[Throwable, PeerIdentityResponse] = Either.catchNonFatal {
    val responseType = in.readByte()
    responseType match {
      case SuccessByte =>
        val pkSize  = in.readInt()
        val pkBytes = ByteBufUtil.getBytes(in.readBytes(pkSize))
        val certificates = if (decodeCerts) {
          val arrLength = in.readShort()
          (0 until arrLength)
            .foldLeft(List.empty[X509Certificate]) {
              case (acc, _) =>
                val certLength = in.readShort()
                val certBytes  = ByteBufUtil.getBytes(in.readBytes(certLength))
                BinarySerializer.x509CertFromBytes(certBytes) :: acc
            }
            .reverse
        } else {
          List.empty[X509Certificate]
        }

        SuccessPeerIdentityResponse(PublicKey(pkBytes), certificates)

      case MessageByte =>
        val messageSize = in.readInt()
        MessagePeerIdentityResponse(new String(ByteBufUtil.getBytes(in.readBytes(messageSize)), StandardCharsets.UTF_8))
      case byte =>
        throw new RuntimeException(
          s"Unknown message byte '$byte' for PeerIdentityResponse. Expected bytes are '$SuccessByte' (success), '$MessageByte' (message)")
    }
  }
}
