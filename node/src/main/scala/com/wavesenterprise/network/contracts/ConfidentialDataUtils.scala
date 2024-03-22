package com.wavesenterprise.network.contracts

import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.protobuf.service.contract.ContractBalanceResponse
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps.DataEntryMap

object ConfidentialDataUtils {

  def entriesToBytes(entries: Seq[DataEntry[_]]): Array[Byte] = {
    val output = newDataOutput()
    BinarySerializer.writeShortIterable(entries.sorted, ContractTransactionEntryOps.writeBytes, output)
    output.toByteArray
  }

  def entryMapToBytes(entryMap: DataEntryMap): Array[Byte] = {
    val output = newDataOutput()
    DataEntryMap.writeBytes(entryMap, output)
    output.toByteArray
  }

  def contractBalancesToBytes(contractBalances: Seq[ContractBalanceResponse]): Array[Byte] = {
    def contractBalanceInfoWriter: BinarySerializer.Writer[ContractBalanceResponse] = (contractBalance, output) => {
      output.write(contractBalance.assetId.map(_.getBytes()).getOrElse(Array[Byte]()))
      output.writeLong(contractBalance.amount)
      output.writeInt(contractBalance.decimals)
    }

    val sortedBalances = contractBalances.sortBy(_.assetId)

    val ndo = newDataOutput()
    BinarySerializer.writeShortIterable(sortedBalances, contractBalanceInfoWriter, ndo)
    ndo.toByteArray

  }

  def fromBytes(bytes: Array[Byte]): List[DataEntry[_]] = {
    val (dataEntries, _) = BinarySerializer.parseShortList(bytes, ContractTransactionEntryOps.parse)

    dataEntries
  }

  def mapFromBytes(bytes: Array[Byte]): DataEntryMap = {
    DataEntryMap.fromBytes(bytes, 0)._1
  }

}
