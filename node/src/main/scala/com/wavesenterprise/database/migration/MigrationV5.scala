package com.wavesenterprise.database.migration

import java.nio.charset.StandardCharsets.UTF_8
import com.google.common.primitives.Ints
import com.wavesenterprise.database.KeyHelpers.addr
import com.wavesenterprise.database.Keys.DataKeysPrefix
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.PresetCF
import com.wavesenterprise.database.rocksdb.{MainDBColumnFamily, MainReadWriteDB}
import com.wavesenterprise.database.{InternalRocksDBSet, Keys, MainDBKey, readStrings, writeStrings}

object MigrationV5 {

  private[migration] object LegacyKeys {

    private val DataKeyChunkCountPrefix: Short = 31
    private val DataKeyChunkPrefix: Short      = 32

    def dataKeyChunkCount(addressId: BigInt): MainDBKey[Int] =
      MainDBKey("data-key-chunk-count", addr(DataKeyChunkCountPrefix, addressId), Option(_).fold(0)(Ints.fromByteArray), Ints.toByteArray)

    def dataKeyChunk(addressId: BigInt, chunkNo: Int): MainDBKey[Seq[String]] =
      MainDBKey("data-key-chunk", addr(DataKeyChunkPrefix, addressId) ++ Ints.toByteArray(chunkNo), readStrings, writeStrings)
  }

  private def dataKeysSet(addressId: BigInt): InternalRocksDBSet[String, MainDBColumnFamily] =
    new InternalRocksDBSet[String, MainDBColumnFamily](
      name = "data-keys",
      columnFamily = PresetCF,
      prefix = addr(DataKeysPrefix, addressId),
      itemEncoder = _.getBytes(UTF_8),
      itemDecoder = new String(_, UTF_8),
      keyConstructors = MainDBKey
    )

  def apply(rw: MainReadWriteDB): Unit = {
    val lastAddressId = rw.get(Keys.lastAddressId).getOrElse(BigInt(0))
    for (addressId <- BigInt(1) to lastAddressId) {
      val dataKeys      = dataKeysSet(addressId)
      val chunkCountKey = LegacyKeys.dataKeyChunkCount(addressId)
      val chunkCount    = rw.get(chunkCountKey)
      val keys = (0 until chunkCount).flatMap { chunk =>
        val chunkKey  = LegacyKeys.dataKeyChunk(addressId, chunk)
        val chunkKeys = rw.get(chunkKey).filter(keyHasValues(rw, addressId))
        rw.delete(chunkKey)
        chunkKeys
      }
      dataKeys.add(rw, keys)
      rw.delete(chunkCountKey)
    }
  }

  private def keyHasValues(rw: MainReadWriteDB, addressId: BigInt)(key: String): Boolean = {
    val historyKey = Keys.dataHistory(addressId, key)
    if (rw.get(historyKey).nonEmpty) true
    else {
      rw.delete(historyKey)
      false
    }
  }
}
