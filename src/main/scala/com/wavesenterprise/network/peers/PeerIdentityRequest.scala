package com.wavesenterprise.network.peers

import com.wavesenterprise.account.Address
import com.wavesenterprise.crypto
import com.wavesenterprise.network.message.MessageSpec.PeerIdentitySpec
import io.netty.buffer.{ByteBuf, ByteBufUtil}
import scorex.crypto.signatures.Signature

case class PeerIdentityRequest(nodeAddress: Address, signature: Signature) {

  lazy val bytes: Array[Byte] = {
    val byteBufferSize = java.lang.Byte.BYTES + Address.AddressLength + crypto.SignatureLength
    val byteBuffer     = java.nio.ByteBuffer.allocate(byteBufferSize)
    byteBuffer.put(PeerIdentitySpec.messageCode)
    byteBuffer.put(nodeAddress.bytes.arr)
    byteBuffer.put(signature)
    byteBuffer.array()
  }

  def encode(out: ByteBuf): ByteBuf = {
    out.writeBytes(bytes)
    out
  }
}

object PeerIdentityRequest {

  def encodeUnsigned(request: PeerIdentityRequest): Array[Byte] = {
    request.nodeAddress.bytes.arr
  }

  def decodeSigned(in: ByteBuf): PeerIdentityRequest = {
    require(in.readByte == PeerIdentitySpec.messageCode, "Wrong message code for PeerIdentityRequest")
    val addressBytes = ByteBufUtil.getBytes(in.readBytes(Address.AddressLength))
    val signature    = ByteBufUtil.getBytes(in.readBytes(crypto.SignatureLength))
    Address
      .fromBytes(addressBytes)
      .map(new PeerIdentityRequest(_, Signature(signature)))
      .fold(err => throw new RuntimeException(err.toString), identity)
  }
}
