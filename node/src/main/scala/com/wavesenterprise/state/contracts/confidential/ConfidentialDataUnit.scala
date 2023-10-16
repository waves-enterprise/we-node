package com.wavesenterprise.state.contracts.confidential

import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.crypto.internals.SaltBytes
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.state.{ByteStr, ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps

trait ConfidentialDataUnit {

  def commitment: Commitment

  def txId: ByteStr

  def contractId: ContractId

  def commitmentKey: SaltBytes

  def entries: List[DataEntry[_]]

  def toBytes: Array[Byte] = {
    val ndo = newDataOutput()
    BinarySerializer.writeShortByteStr(commitment.hash, ndo)
    BinarySerializer.writeShortByteStr(txId, ndo)
    BinarySerializer.writeShortByteStr(contractId.byteStr, ndo)
    BinarySerializer.writeShortByteStr(commitmentKey.bytes, ndo)
    BinarySerializer.writeShortIterable(entries.sorted, ContractTransactionEntryOps.writeBytes, ndo)
    ndo.toByteArray
  }

}
