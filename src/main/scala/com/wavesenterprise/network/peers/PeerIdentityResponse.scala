package com.wavesenterprise.network.peers

import java.nio.charset.StandardCharsets

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto.PublicKey
import io.netty.buffer.{ByteBuf, ByteBufUtil}

sealed trait PeerIdentityResponse {

  def bytes: Array[Byte]

  def encode(out: ByteBuf): ByteBuf
}

final case class SuccessPeerIdentityResponse(pk: PublicKey) extends PeerIdentityResponse {

  lazy val bytes: Array[Byte] = {
    val byteBuffer = java.nio.ByteBuffer.allocate(java.lang.Byte.BYTES + java.lang.Integer.BYTES + pk.getEncoded.length)
    byteBuffer.put(PeerIdentityResponse.SuccessByte)
    byteBuffer.putInt(pk.getEncoded.length)
    byteBuffer.put(pk.getEncoded)
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
        val pkSize = in.readInt()
        SuccessPeerIdentityResponse(PublicKey(ByteBufUtil.getBytes(in.readBytes(pkSize))))
      case MessageByte =>
        val messageSize = in.readInt()
        MessagePeerIdentityResponse(new String(ByteBufUtil.getBytes(in.readBytes(messageSize)), StandardCharsets.UTF_8))
      case byte =>
        throw new RuntimeException(
          s"Unknown message byte '$byte' for PeerIdentityResponse. Expected bytes are '$SuccessByte' (success), '$MessageByte' (message)")
    }
  }
}
