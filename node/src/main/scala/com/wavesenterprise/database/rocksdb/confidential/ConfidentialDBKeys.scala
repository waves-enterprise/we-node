package com.wavesenterprise.database.rocksdb.confidential

import java.nio.charset.StandardCharsets.UTF_8
import com.google.common.primitives.{Ints, Shorts}
import com.wavesenterprise.database.rocksdb.ConfidentialDBColumnFamily
import com.wavesenterprise.database.KeyHelpers.{bytes, hBytes}
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialKeyHelpers.{historyKey, intKey}
import com.wavesenterprise.state.{ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps
import ConfidentialDBColumnFamily._
import com.wavesenterprise.database.RocksDBSet.ConfidentialRocksDBSet
import com.wavesenterprise.database.RocksDBSet

object ConfidentialDBKeys {

  private[database] val PrefixLength: Int = 2

  private[database] val SchemaVersionPrefix: Short = 0

  private val ConfidentialKeysPrefix: Short        = 1
  private val ConfidentialDataHistoryPrefix: Short = 2
  private val ConfidentialDataPrefix: Short        = 3
  private val HeightPrefix: Short                  = 4

  val schemaVersion: ConfidentialKey[Option[Int]] =
    ConfidentialDBKey.opt("schema-version", Shorts.toByteArray(SchemaVersionPrefix), Ints.fromByteArray, Ints.toByteArray)

  val height: ConfidentialKey[Int] = intKey("height", HeightPrefix)

  def contractKeys(contractId: ContractId, storage: ConfidentialRocksDBStorage): ConfidentialRocksDBSet[String] =
    RocksDBSet.newConfidential(
      name = "contract-keys",
      columnFamily = ConfidentialStateCF,
      prefix = bytes(ConfidentialKeysPrefix, contractId.byteStr.arr),
      storage = storage,
      itemEncoder = _.getBytes(UTF_8),
      itemDecoder = new String(_, UTF_8)
    )

  def contractDataHistory(contractId: ContractId, key: String): ConfidentialKey[Seq[Int]] =
    historyKey("contract-data-history", ConfidentialStateCF, ConfidentialDataHistoryPrefix, contractId.byteStr.arr ++ key.getBytes(UTF_8))

  def contractData(contractId: ContractId, key: String)(height: Int): ConfidentialKey[Option[DataEntry[_]]] =
    ConfidentialDBKey.opt(
      "contract-data",
      ConfidentialStateCF,
      hBytes(ConfidentialDataPrefix, height, contractId.byteStr.arr ++ key.getBytes(UTF_8)),
      ContractTransactionEntryOps.parseValue(key, _, 0)._1,
      ContractTransactionEntryOps.valueBytes
    )

}
