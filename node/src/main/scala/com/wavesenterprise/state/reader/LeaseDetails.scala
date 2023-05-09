package com.wavesenterprise.state.reader

import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.account.{AddressOrAlias, PublicKeyAccount}
import com.wavesenterprise.crypto.{DigestSize, KeyLength}
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.lease.LeaseTransaction

case class LeaseDetails(
    sender: PublicKeyAccount,
    recipient: AddressOrAlias,
    height: Int,
    amount: Long,
    isActive: Boolean,
    leaseTxId: Option[ByteStr] // non-empty if lease was created by native token operation (in other words when leaseId is not equal to leaseTxId)
) {
  def toBytes: Array[Byte] = {
    val output = newDataOutput()
    output.write(sender.publicKey.getEncoded)
    output.write(recipient.bytes.arr)
    output.writeInt(height)
    output.writeLong(amount)
    output.writeBoolean(isActive)
    leaseTxId.foreach(txId => output.write(txId.arr))

    output.toByteArray
  }
}

object LeaseDetails {

  def fromBytes(bytes: Array[Byte]): LeaseDetails = {

    val senderBytes                    = bytes.take(KeyLength)
    val (recipient, addressOrAliasEnd) = AddressOrAlias.fromBytesUnsafe(bytes, KeyLength)
    val (height, heightEnd)            = BinarySerializer.parseInt(bytes, addressOrAliasEnd)
    val (amount, amountEnd)            = BinarySerializer.parseLong(bytes, heightEnd)
    val (isActive, isActiveEnd)        = (bytes(amountEnd) == 1) -> (amountEnd + 1)

    val leaseTxId = if (bytes.length > isActiveEnd) {
      Some(ByteStr(bytes.slice(isActiveEnd, DigestSize)))
    } else {
      None
    }

    LeaseDetails(
      PublicKeyAccount(senderBytes),
      recipient,
      height,
      amount,
      isActive,
      leaseTxId
    )

  }

  def fromLeaseTx(tx: LeaseTransaction, height: Int): LeaseDetails = {
    LeaseDetails(
      tx.sender,
      tx.recipient,
      height,
      tx.amount,
      true,
      None
    )
  }
}
