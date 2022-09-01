package com.wavesenterprise.network.handshake

import cats.implicits.toShow
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams.newDataOutput
import com.google.common.primitives.{Chars, Ints, Longs, Shorts}
import com.wavesenterprise.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.{ApplicationInfo, NodeVersion, crypto}
import io.netty.buffer.ByteBuf
import monix.eval.Coeval
import scorex.crypto.signatures.Signature

import java.net.{InetAddress, InetSocketAddress}
import java.util
import scala.util.hashing.MurmurHash3

//noinspection UnstableApiUsage
case class SignedHandshakeV3(payload: HandshakeV3Payload, payloadBytes: Array[Byte], nodeOwnerAddress: Address, signature: Signature)
    extends SignedHandshake {

  override def nodeVersion: NodeVersion = payload.nodeVersion

  override def consensusType: String = payload.consensusType

  override def nodeName: String = payload.nodeName

  override def nodeNonce: Long = payload.nodeNonce

  override def declaredAddress: Option[InetSocketAddress] = payload.declaredAddress

  override def sessionPubKey: PublicKeyAccount = payload.sessionPubKey

  override def chainId: Char = payload.chainId

  override val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    val payloadBytes = payload.bytes()
    val output       = newDataOutput(1 + Ints.BYTES + payloadBytes.length + SignedHandshake.AddressLength + SignedHandshake.SignatureLength)

    output.writeByte(SignedHandshakeV3.typeByte)
    BinarySerializer.writeBigByteArray(payload.bytes(), output)
    output.write(nodeOwnerAddress.bytes.arr)
    output.write(signature)

    output.toByteArray
  }

  override def isSignatureValid(senderPublicKey: PublicKeyAccount): Boolean =
    crypto.verify(signature, payloadBytes, senderPublicKey.publicKey)

  override def toString: String = {
    def declaredAddressStr = declaredAddress.fold("-")(_.toString)

    s"SignedHandshakeV3(nodeVersion: ${nodeVersion.show}, consensus: $consensusType, nodeName: $nodeName, nodeNonce: $nodeNonce," +
      s" declaredAddress: $declaredAddressStr, nodeOwnerAddress: $nodeOwnerAddress, sessionPubKey: $sessionPubKey)"
  }

  override def equals(other: Any): Boolean = other match {
    case that: SignedHandshakeV3 =>
      eq(that) || (that canEqual this) &&
        payload == that.payload &&
        util.Arrays.equals(payloadBytes, that.payloadBytes) &&
        nodeOwnerAddress == that.nodeOwnerAddress &&
        util.Arrays.equals(signature, that.signature)
    case _ => false
  }

  override def hashCode(): Int =
    MurmurHash3.orderedHash(Seq(payload, payloadBytes.toSeq, nodeOwnerAddress, signature.toSeq))
}

object SignedHandshakeV3 {

  val typeByte: Byte = 3

  def createAndSign(chainId: Char,
                    nodeVersion: NodeVersion,
                    consensusType: String,
                    declaredAddress: Option[InetSocketAddress],
                    nodeNonce: Long,
                    nodeName: String,
                    sessionPubKey: PublicKeyAccount,
                    ownerKey: PrivateKeyAccount): SignedHandshakeV3 = {
    val payload                   = HandshakeV3Payload(chainId, nodeVersion, consensusType, declaredAddress, nodeNonce, nodeName, sessionPubKey)
    val payloadBytes              = payload.bytes()
    val handshakeWithoutSignature = SignedHandshakeV3(payload, payloadBytes, ownerKey.toAddress, Signature(Array.empty[Byte]))
    val signature                 = crypto.sign(ownerKey, payloadBytes)
    handshakeWithoutSignature.copy(signature = signature)
  }

  def createAndSign(applicationInfo: ApplicationInfo, sessionPubKey: PublicKeyAccount, ownerKey: PrivateKeyAccount): SignedHandshakeV3 =
    createAndSign(
      applicationInfo.chainId,
      applicationInfo.nodeVersion,
      applicationInfo.consensusType,
      applicationInfo.declaredAddress,
      applicationInfo.nodeNonce,
      applicationInfo.nodeName,
      sessionPubKey,
      ownerKey
    )

  private[handshake] def decodeWithoutType(in: ByteBuf): SignedHandshakeV3 = {
    val payloadSize  = in.readInt()
    val payloadBytes = new Array[Byte](payloadSize)
    in.readBytes(payloadBytes)

    val addressBytes = new Array[Byte](SignedHandshake.AddressLength)
    in.readBytes(addressBytes)

    val signatureBytes = new Array[Byte](SignedHandshake.SignatureLength)
    in.readBytes(signatureBytes)

    val payload          = HandshakeV3Payload.parse(payloadBytes)
    val nodeOwnerAddress = Address.fromBytesUnsafe(addressBytes)

    val signature = Signature(signatureBytes)
    SignedHandshakeV3(payload, payloadBytes, nodeOwnerAddress, signature)
  }

}

//noinspection UnstableApiUsage
case class HandshakeV3Payload(
    chainId: Char,
    nodeVersion: NodeVersion,
    consensusType: String,
    declaredAddress: Option[InetSocketAddress],
    nodeNonce: Long,
    nodeName: String,
    sessionPubKey: PublicKeyAccount
) {
  val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    val consensusTypeBytes = consensusType.getBytes(Charsets.UTF_8)

    val maybeAddressBytes = declaredAddress.flatMap { inetAddress =>
      Option(inetAddress.getAddress).map { address =>
        val portBytes    = Ints.toByteArray(inetAddress.getPort)
        val addressBytes = address.getAddress
        Array.concat(portBytes, addressBytes)
      }
    }

    val nodeNameBytes = nodeName.getBytes(Charsets.UTF_8)

    val sessionPubKeyBytes = sessionPubKey.publicKey.getEncoded

    val output = newDataOutput(
      Chars.BYTES + Ints.BYTES * 3 + Shorts.BYTES + consensusTypeBytes.length +
        maybeAddressBytes.fold(1)(1 + Shorts.BYTES + _.length) + Longs.BYTES + Shorts.BYTES + nodeNameBytes.length +
        sessionPubKeyBytes.length
    )

    output.writeChar(chainId)
    output.writeInt(nodeVersion.majorVersion)
    output.writeInt(nodeVersion.minorVersion)
    output.writeInt(nodeVersion.patchVersion)
    BinarySerializer.writeShortByteArray(consensusTypeBytes, output)
    BinarySerializer.writeByteIterable(maybeAddressBytes, BinarySerializer.writeShortByteArray, output)
    output.writeLong(nodeNonce)
    BinarySerializer.writeShortByteArray(nodeNameBytes, output)
    output.write(sessionPubKeyBytes)

    output.toByteArray
  }
}

object HandshakeV3Payload {

  def parse(bytes: Array[Byte]): HandshakeV3Payload = {
    val (chainId, chainIdEnd)                       = Chars.fromByteArray(bytes.slice(0, Chars.BYTES)) -> Chars.BYTES
    val (majorVersion, majorVersionEnd)             = Ints.fromByteArray(bytes.slice(chainIdEnd, chainIdEnd + Ints.BYTES)) -> (chainIdEnd + Ints.BYTES)
    val (minorVersion, minorVersionEnd)             = Ints.fromByteArray(bytes.slice(majorVersionEnd, majorVersionEnd + Ints.BYTES)) -> (majorVersionEnd + Ints.BYTES)
    val (pathVersion, pathVersionEnd)               = Ints.fromByteArray(bytes.slice(minorVersionEnd, minorVersionEnd + Ints.BYTES)) -> (minorVersionEnd + Ints.BYTES)
    val (consensusTypeBytes, consensusTypeBytesEnd) = BinarySerializer.parseShortByteArray(bytes, pathVersionEnd)
    val (maybeAddressBytes, maybeAddressBytesEnd)   = BinarySerializer.parseOption(bytes, BinarySerializer.parseShortByteArray, consensusTypeBytesEnd)
    val (nodeNonce, nodeNonceEnd)                   = Longs.fromByteArray(bytes.slice(maybeAddressBytesEnd, maybeAddressBytesEnd + Longs.BYTES)) -> (maybeAddressBytesEnd + Longs.BYTES)
    val (nodeNameBytes, nodeNameBytesEnd)           = BinarySerializer.parseShortByteArray(bytes, nodeNonceEnd)
    val sessionPubKeyBytes                          = bytes.slice(nodeNameBytesEnd, nodeNameBytesEnd + SignedHandshake.PubKeyLength)

    val nodeAddress   = NodeVersion(majorVersion, minorVersion, pathVersion)
    val consensusType = new String(consensusTypeBytes, Charsets.UTF_8)
    val nodeName      = new String(nodeNameBytes, Charsets.UTF_8)
    val publicKey     = PublicKeyAccount.fromSessionPublicKey(sessionPubKeyBytes)
    val maybeAddress = maybeAddressBytes.map { addressBytes =>
      val (portBytes, inetBytes) = addressBytes.splitAt(Ints.BYTES)
      val port                   = Ints.fromByteArray(portBytes)
      val inetAddress            = InetAddress.getByAddress(inetBytes)
      new InetSocketAddress(inetAddress, port)
    }

    HandshakeV3Payload(chainId, nodeAddress, consensusType, maybeAddress, nodeNonce, nodeName, publicKey)
  }
}
