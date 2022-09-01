package com.wavesenterprise.database.migration

import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.primitives.Ints
import com.wavesenterprise.database.KeyHelpers.addr
import com.wavesenterprise.database.Keys.DataKeysPrefix
import com.wavesenterprise.database.rocksdb.ColumnFamily.DefaultCF
import com.wavesenterprise.database.rocksdb.RW
import com.wavesenterprise.database.{InternalRocksDBSet, Key, Keys, readStrings, writeStrings}

object MigrationV5 {

  private[migration] object LegacyKeys {

    private val DataKeyChunkCountPrefix: Short = 31
    private val DataKeyChunkPrefix: Short      = 32

    def dataKeyChunkCount(addressId: BigInt): Key[Int] =
      Key("data-key-chunk-count", addr(DataKeyChunkCountPrefix, addressId), Option(_).fold(0)(Ints.fromByteArray), Ints.toByteArray)

    def dataKeyChunk(addressId: BigInt, chunkNo: Int): Key[Seq[String]] =
      Key("data-key-chunk", addr(DataKeyChunkPrefix, addressId) ++ Ints.toByteArray(chunkNo), readStrings, writeStrings)
  }

  private def dataKeysSet(addressId: BigInt): InternalRocksDBSet[String] =
    new InternalRocksDBSet[String](
      name = "data-keys",
      columnFamily = DefaultCF,
      prefix = addr(DataKeysPrefix, addressId),
      itemEncoder = _.getBytes(UTF_8),
      itemDecoder = new String(_, UTF_8)
    )

  def apply(rw: RW): Unit = {
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

  private def keyHasValues(rw: RW, addressId: BigInt)(key: String): Boolean = {
    val historyKey = Keys.dataHistory(addressId, key)
    if (rw.get(historyKey).nonEmpty) true
    else {
      rw.delete(historyKey)
      false
    }
  }
}
