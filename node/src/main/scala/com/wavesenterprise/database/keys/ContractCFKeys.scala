package com.wavesenterprise.database.keys

import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.primitives.Shorts
import com.wavesenterprise.database.KeyHelpers.{bytes, hBytes, hash, historyKey}
import com.wavesenterprise.database.rocksdb.ColumnFamily.ContractCF
import com.wavesenterprise.database.rocksdb.RocksDBStorage
import com.wavesenterprise.database.{Key, RocksDBSet, readContractInfo, writeContractInfo}
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps

object ContractCFKeys {

  val ContractIdsPrefix: Short         = 1
  val ContractHistoryPrefix: Short     = 2
  val ContractPrefix: Short            = 3
  val ContractDataHistoryPrefix: Short = 6
  val ContractDataPrefix: Short        = 7
  val ExecutedTxMappingPrefix: Short   = 8
  val ContractKeysPrefix: Short        = 9

  def contractIdsSet(storage: RocksDBStorage): RocksDBSet[ByteStr] =
    new RocksDBSet[ByteStr](
      name = "contract-ids",
      columnFamily = ContractCF,
      prefix = Shorts.toByteArray(ContractIdsPrefix),
      storage = storage,
      itemEncoder = (_: ByteStr).arr,
      itemDecoder = ByteStr(_)
    )

  def contractHistory(contractId: ByteStr): Key[Seq[Int]] = historyKey("contract-history", ContractCF, ContractHistoryPrefix, contractId.arr)

  def contract(contractId: ByteStr)(height: Int): Key[Option[ContractInfo]] =
    Key.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), readContractInfo, writeContractInfo)

  def contractDataHistory(contractId: ByteStr, key: String): Key[Seq[Int]] =
    historyKey("contract-data-history", ContractCF, ContractDataHistoryPrefix, contractId.arr ++ key.getBytes(UTF_8))

  def contractData(contractId: ByteStr, key: String)(height: Int): Key[Option[DataEntry[_]]] =
    Key.opt(
      "contract-data",
      ContractCF,
      hBytes(ContractDataPrefix, height, contractId.arr ++ key.getBytes(UTF_8)),
      ContractTransactionEntryOps.parseValue(key, _, 0)._1,
      ContractTransactionEntryOps.valueBytes
    )

  def executedTxIdFor(txId: ByteStr): Key[Option[ByteStr]] =
    Key.opt("executed-tx-id-for", ContractCF, hash(ExecutedTxMappingPrefix, txId), ByteStr(_), _.arr)

  def contractKeys(contractId: ByteStr, storage: RocksDBStorage): RocksDBSet[String] =
    new RocksDBSet[String](
      name = "contract-keys",
      columnFamily = ContractCF,
      prefix = bytes(ContractKeysPrefix, contractId.arr),
      storage = storage,
      itemEncoder = _.getBytes(UTF_8),
      itemDecoder = new String(_, UTF_8)
    )
}
