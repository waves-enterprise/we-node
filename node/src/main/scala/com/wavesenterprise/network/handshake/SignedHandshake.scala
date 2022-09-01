package com.wavesenterprise.network.handshake

import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.{NodeVersion, crypto}
import io.netty.buffer.ByteBuf
import monix.eval.Coeval

import java.net.InetSocketAddress

trait SignedHandshake {

  def nodeOwnerAddress: Address

  def nodeVersion: NodeVersion

  def consensusType: String

  def nodeName: String

  def nodeNonce: Long

  def declaredAddress: Option[InetSocketAddress]

  def sessionPubKey: PublicKeyAccount

  def chainId: Char

  def bytes: Coeval[Array[Byte]]

  def encode(out: ByteBuf): ByteBuf = out.writeBytes(bytes())

  def isSignatureValid(senderPublicKey: PublicKeyAccount): Boolean
}

object SignedHandshake {
  val AddressLength: Int   = Address.AddressLength
  val SignatureLength: Int = crypto.SignatureLength
  val PubKeyLength: Int    = crypto.SessionKeyLength

  val AllTypeBytes = Set(SignedHandshakeV3.typeByte)

  def decode(in: ByteBuf): SignedHandshake = {
    val handshakeTypeByte = in.readByte()

    handshakeTypeByte match {
      case SignedHandshakeV3.typeByte => SignedHandshakeV3.decodeWithoutType(in)
      case other                      => throw new IllegalArgumentException(s"Unknown handshake type byte: $other")
    }
  }

  class InvalidHandshakeException(msg: String) extends IllegalArgumentException(msg)
}
