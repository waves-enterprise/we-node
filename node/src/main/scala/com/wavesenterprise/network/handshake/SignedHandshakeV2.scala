package com.wavesenterprise.network.handshake

import cats.implicits.toShow
import cats.syntax.either._
import com.google.common.base.Charsets
import com.google.common.primitives.Bytes
import com.wavesenterprise.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.network.handshake.SignedHandshake.InvalidHandshakeException
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.{ApplicationInfo, NodeVersion, crypto}
import io.netty.buffer.ByteBuf
import monix.eval.Coeval
import scorex.crypto.signatures.Signature

import java.net.{InetAddress, InetSocketAddress}
import java.util
import scala.util.Try
import scala.util.hashing.MurmurHash3

case class SignedHandshakeV2(applicationInfo: ApplicationInfo, nodeOwnerAddress: Address, sessionPubKey: PublicKeyAccount, signature: Signature)
    extends SignedHandshake {

  def applicationName: String = applicationInfo.applicationName

  def nodeVersion: NodeVersion = applicationInfo.nodeVersion

  def consensusType: String = applicationInfo.consensusType

  def nodeName: String = applicationInfo.nodeName

  def nodeNonce: Long = applicationInfo.nodeNonce

  def declaredAddress: Option[InetSocketAddress] = applicationInfo.declaredAddress

  val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    val addressBytes = nodeOwnerAddress.bytes.arr

    require(
      addressBytes.length == SignedHandshake.AddressLength,
      s"The addressBytes.length (${addressBytes.length}) is not equal to ${SignedHandshake.AddressLength}!"
    )
    require(signature.length == SignedHandshake.SignatureLength,
            s"The signature.length (${signature.length}) is not equal to ${SignedHandshake.SignatureLength}!")

    val commonBytes    = SignedHandshakeV2.encodeWithoutSignature(this)
    val byteBufferSize = commonBytes.length + SignedHandshake.AddressLength + SignedHandshake.PubKeyLength + SignedHandshake.SignatureLength
    val bb             = java.nio.ByteBuffer.allocate(byteBufferSize)

    bb.put(commonBytes)
    bb.put(addressBytes)
    bb.put(sessionPubKey.publicKey.getEncoded)
    bb.put(signature)

    bb.array()
  }

  override def toString: String = {
    def declaredAddressStr = declaredAddress.fold("-")(_.toString)

    s"SignedHandshakeV2(nodeVersion: ${nodeVersion.show}, consensus: $consensusType, nodeName: $nodeName, nodeNonce: $nodeNonce," +
      s" nodeAddress: $declaredAddressStr, address: $nodeOwnerAddress, sessionPubKey: $sessionPubKey)"
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: SignedHandshakeV2 =>
      this.applicationName == other.applicationName &&
        this.nodeVersion == other.nodeVersion &&
        this.consensusType == other.consensusType &&
        this.nodeName == other.nodeName &&
        this.nodeNonce == other.nodeNonce &&
        this.declaredAddress == other.declaredAddress &&
        this.nodeOwnerAddress == other.nodeOwnerAddress &&
        this.sessionPubKey.publicKey == other.sessionPubKey.publicKey &&
        util.Arrays.equals(this.signature, other.signature)
    case _ => false
  }

  override def hashCode(): Int =
    MurmurHash3.orderedHash(Seq(applicationInfo, nodeOwnerAddress, signature.toSeq))

  def isSignatureValid(senderPublicKey: PublicKeyAccount): Boolean = {
    val commonBytes = SignedHandshakeV2.encodeWithoutSignature(this)
    val bytesToSign = Bytes.concat(commonBytes, sessionPubKey.publicKey.getEncoded)
    crypto.verify(signature, bytesToSign, senderPublicKey.publicKey)
  }

  /** Legacy implementation. Needed to maintain backward compatibility */
  override def chainId: Char = ApplicationInfo.extractChainIdFromAppName(applicationName)
}

object SignedHandshakeV2 extends ScorexLogging {

  val typeByte: Byte = 2

  def createAndSign(applicationInstanceInfo: ApplicationInfo, sessionPubKey: PublicKeyAccount, ownerKey: PrivateKeyAccount): SignedHandshakeV2 = {
    val handshakeWithoutSignature =
      SignedHandshakeV2(applicationInstanceInfo, ownerKey.toAddress, sessionPubKey, Signature(Array.empty[Byte]))

    val signedBytes = SignedHandshakeV2.encodeWithoutSignature(handshakeWithoutSignature)
    val bytesToSign = Bytes.concat(signedBytes, sessionPubKey.publicKey.getEncoded)
    val signature   = crypto.sign(ownerKey, bytesToSign)
    handshakeWithoutSignature.copy(signature = signature)
  }

  def encodeWithoutSignature(handshake: SignedHandshakeV2): Array[Byte] = {
    val applicationNameBytes = handshake.applicationName.getBytes(Charsets.UTF_8)
    require(applicationNameBytes.length <= Byte.MaxValue, "The application name is too long!")

    val consensusNameBytes = handshake.consensusType.getBytes(Charsets.UTF_8)
    require(consensusNameBytes.length <= Byte.MaxValue, "A consensus name is too long!")

    val nodeNameBytes = handshake.nodeName.getBytes(Charsets.UTF_8)
    require(nodeNameBytes.length <= Byte.MaxValue, "A node name is too long!")

    val peer = for {
      inetAddress <- handshake.declaredAddress
      address     <- Option(inetAddress.getAddress)
    } yield (address.getAddress, inetAddress.getPort)
    val peerSize = peer match {
      case None                    => Integer.BYTES
      case Some((addressBytes, _)) => Integer.BYTES + addressBytes.length + Integer.BYTES
    }

    val byteBufferSize =
      // one byte for type and for app name length
      2 * java.lang.Byte.BYTES +
        // length of app name
        applicationNameBytes.length +
        // 12 bytes for 3 applicationVersion integers
        3 * Integer.BYTES +
        // consensusName and its length
        1 + consensusNameBytes.length +
        // nodeName and its length
        1 + nodeNameBytes.length +
        java.lang.Long.BYTES + peerSize
    val bb = java.nio.ByteBuffer.allocate(byteBufferSize)

    bb.put(typeByte)
    bb.put(applicationNameBytes.length.toByte)
    bb.put(applicationNameBytes)

    bb.putInt(handshake.nodeVersion.majorVersion)
    bb.putInt(handshake.nodeVersion.minorVersion)
    bb.putInt(handshake.nodeVersion.patchVersion)

    bb.put(consensusNameBytes.length.toByte)
    bb.put(consensusNameBytes)

    bb.put(nodeNameBytes.length.toByte)
    bb.put(nodeNameBytes)

    bb.putLong(handshake.nodeNonce)

    peer match {
      case None =>
        bb.putInt(0)
      case Some((addressBytes, peerPort)) =>
        bb.putInt(addressBytes.length + Integer.BYTES)
        bb.put(addressBytes)
        bb.putInt(peerPort)
    }

    bb.array()
  }

  private[handshake] def decodeWithoutType(in: ByteBuf): SignedHandshakeV2 = {
    val appNameSize = in.readByte()

    if (appNameSize < 0 || appNameSize > Byte.MaxValue) {
      throw new InvalidHandshakeException(s"An invalid application name's size: $appNameSize")
    }
    val appName     = in.readSlice(appNameSize).toString(Charsets.UTF_8)
    val nodeVersion = NodeVersion(in.readInt(), in.readInt(), in.readInt())

    val consensusNameSize = in.readByte()
    if (consensusNameSize < 0 || consensusNameSize > Byte.MaxValue) {
      throw new InvalidHandshakeException(s"An invalid node's consensus name size: $consensusNameSize")
    }
    val consensusName = in.readSlice(consensusNameSize).toString(Charsets.UTF_8)

    val nodeNameSize = in.readByte()
    if (nodeNameSize < 0 || nodeNameSize > Byte.MaxValue) {
      throw new InvalidHandshakeException(s"An invalid node name's size: $nodeNameSize")
    }
    val nodeName = in.readSlice(nodeNameSize).toString(Charsets.UTF_8)

    val nonce = in.readLong()

    val declaredAddressLength = in.readInt()
    // 0 for no declared address, 8 for ipv4 address + port, 20 for ipv6 address + port
    if (declaredAddressLength != 0 && declaredAddressLength != 8 && declaredAddressLength != 20) {
      throw new InvalidHandshakeException(s"An invalid declared address length: $declaredAddressLength")
    }
    val isa =
      if (declaredAddressLength == 0) None
      else {
        val addressBytes = new Array[Byte](declaredAddressLength - Integer.BYTES)
        in.readBytes(addressBytes)
        val address = InetAddress.getByAddress(addressBytes)
        val port    = in.readInt()
        Some(new InetSocketAddress(address, port))
      }

    val maybeHandshake = for {
      addressBytes <- readFixedLengthBytes(
        in,
        SignedHandshake.AddressLength,
        remains => s"Not enough bytes for address: remainBytes = $remains, addressLength = ${SignedHandshake.AddressLength}")
      address <- Address
        .fromBytes(addressBytes)
        .leftMap(error => s"Can't construct address from addressBytes, message: '${error.message}'")
      pubKeyBytes <- readFixedLengthBytes(in, {
                                            SignedHandshake.PubKeyLength
                                          },
                                          remains =>
                                            s"Not enough bytes for public key: remainBytes = $remains, public key = ${SignedHandshake.PubKeyLength}")
      publicKey = PublicKeyAccount.fromSessionPublicKey(pubKeyBytes)
      signatureBytes <- readFixedLengthBytes(
        in,
        SignedHandshake.SignatureLength,
        remains => s"Not enough bytes for signature: remainBytes = $remains, signatureLength = ${SignedHandshake.SignatureLength}"
      )
      signature       = Signature(signatureBytes)
      applicationInfo = ApplicationInfo(appName, nodeVersion, consensusName, nodeName, nonce, isa)
    } yield SignedHandshakeV2(applicationInfo, address, publicKey, signature)

    maybeHandshake.fold(error => throw new InvalidHandshakeException(error), identity)
  }

  private def readFixedLengthBytes(bb: ByteBuf, expectedLength: Int, errorForGivenRemains: Int => String): Either[String, Array[Byte]] = {
    val remainingBytesBeforeRead = bb.readableBytes()
    Either
      .cond(remainingBytesBeforeRead >= expectedLength, (), errorForGivenRemains(remainingBytesBeforeRead))
      .flatMap { _ =>
        val readBytes = new Array[Byte](expectedLength)
        Try(bb.readBytes(readBytes)).toEither
          .bimap(ex => s"Error reading bytes: ${ex.getMessage}", _ => readBytes)
      }
  }
}
