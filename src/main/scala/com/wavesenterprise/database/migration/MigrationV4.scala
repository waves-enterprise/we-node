package com.wavesenterprise.database.migration

import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.primitives.{Ints, Shorts}
import com.wavesenterprise.database.KeyHelpers.{bytes, hash}
import com.wavesenterprise.database.keys.ContractCFKeys.{ContractIdsPrefix, ContractKeysPrefix}
import com.wavesenterprise.database.rocksdb.ColumnFamily.ContractCF
import com.wavesenterprise.database.rocksdb.RW
import com.wavesenterprise.database.{InternalRocksDBSet, Key, WEKeys, readStrings, writeStrings}
import com.wavesenterprise.state.ByteStr

object MigrationV4 {

  private[migration] object LegacyKeys {

    private val ContractKeyChunkCountPrefix: Short = 4
    private val ContractKeyChunkPrefix: Short      = 5

    def contractKeyChunkCount(contractId: ByteStr): Key[Int] =
      Key("contract-key-chunk-count",
          ContractCF,
          hash(ContractKeyChunkCountPrefix, contractId),
          Option(_).fold(0)(Ints.fromByteArray),
          Ints.toByteArray)

    def contractDataKeyChunk(contractId: ByteStr, chunkNo: Int): Key[Vector[String]] =
      Key("contract-data-key-chunk-count",
          ContractCF,
          hash(ContractKeyChunkPrefix, contractId) ++ Ints.toByteArray(chunkNo),
          readStrings,
          writeStrings)
  }

  private val ContractsIdsSet = new InternalRocksDBSet[ByteStr](
    name = "contract-ids",
    columnFamily = ContractCF,
    prefix = Shorts.toByteArray(ContractIdsPrefix),
    itemEncoder = (_: ByteStr).arr,
    itemDecoder = ByteStr(_)
  )

  private def contractKeysSet(contractId: ByteStr): InternalRocksDBSet[String] = {
    new InternalRocksDBSet[String](
      name = "contract-keys",
      columnFamily = ContractCF,
      prefix = bytes(ContractKeysPrefix, contractId.arr),
      itemEncoder = _.getBytes(UTF_8),
      itemDecoder = new String(_, UTF_8)
    )
  }

  def apply(rw: RW): Unit = {
    val contracts = ContractsIdsSet.members(rw)
    for (contract <- contracts) {
      val contractKeys  = contractKeysSet(contract)
      val chunkCountKey = LegacyKeys.contractKeyChunkCount(contract)
      val chunkCount    = rw.get(chunkCountKey)
      val keys = (0 until chunkCount).flatMap { chunk =>
        val chunkKey  = LegacyKeys.contractDataKeyChunk(contract, chunk)
        val chunkKeys = rw.get(chunkKey).filter(keyHasValues(rw, contract))
        rw.delete(chunkKey)
        chunkKeys
      }
      contractKeys.add(rw, keys)
      rw.delete(chunkCountKey)
    }
  }

  private def keyHasValues(rw: RW, contractId: ByteStr)(key: String): Boolean = {
    val historyKey = WEKeys.contractDataHistory(contractId, key)
    if (rw.get(historyKey).nonEmpty) true
    else {
      rw.delete(historyKey)
      false
    }
  }
}
