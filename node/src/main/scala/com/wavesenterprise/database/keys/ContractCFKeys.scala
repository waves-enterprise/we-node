package com.wavesenterprise.database.keys

import com.google.common.primitives.{Longs, Shorts}
import com.wavesenterprise.database.KeyHelpers._
import com.wavesenterprise.database._
import com.wavesenterprise.database.rocksdb.ColumnFamily.ContractCF
import com.wavesenterprise.database.rocksdb.RocksDBStorage
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps

import java.nio.charset.StandardCharsets.UTF_8

object ContractCFKeys {

  val ContractIdsPrefix: Short                 = 1
  val ContractHistoryPrefix: Short             = 2
  val ContractPrefix: Short                    = 3
  val ContractDataHistoryPrefix: Short         = 6
  val ContractDataPrefix: Short                = 7
  val ExecutedTxMappingPrefix: Short           = 8
  val ContractKeysPrefix: Short                = 9
  val ContractIdStateIdPrefix: Short           = 10
  val ContractStateIdIdPrefix: Short           = 11
  val ContractWestBalanceHistoryPrefix: Short  = 12
  val ContractWestBalancePrefix: Short         = 13
  val ContractAssetListPrefix: Short           = 14
  val ContractAssetsBalancePrefix: Short       = 15
  val ContractAssetBalanceHistoryPrefix: Short = 16
  val ContractAssetBalancePrefix: Short        = 17
  val ContractAssetInfoHistoryPrefix: Short    = 18
  val LastContractStateIdPrefix: Short         = 19
  val ChangedContractsPrefix: Short            = 20

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

  def contractIdToStateId(contractId: ByteStr): Key[Option[BigInt]] =
    Key.opt(
      name = "contract-id-to-state-id",
      columnFamily = ContractCF,
      key = bytes(ContractIdStateIdPrefix, contractId.arr),
      parser = BigInt(_),
      encoder = _.toByteArray
    )

  def stateIdToContractId(stateId: BigInt): Key[ByteStr] =
    Key(
      name = "state-id-to-contract-id",
      columnFamily = ContractCF,
      key = bytes(ContractStateIdIdPrefix, stateId.toByteArray),
      decoder = ByteStr(_),
      encoder = _.arr
    )

  def contractWestBalanceHistory(stateId: BigInt): Key[Seq[Int]] =
    historyKey(
      name = "contract-west-balance-history",
      columnFamily = ContractCF,
      prefix = ContractWestBalanceHistoryPrefix,
      stateId.toByteArray
    )

  def contractWestBalance(stateId: BigInt)(height: Int): Key[Long] =
    Key(
      name = "contract-west-balance",
      columnFamily = ContractCF,
      key = hBytes(ContractWestBalancePrefix, height, stateId.toByteArray),
      decoder = Option(_).fold(0L)(Longs.fromByteArray),
      encoder = Longs.toByteArray
    )

  def contractAssetList(stateId: BigInt): Key[Set[ByteStr]] =
    Key(
      name = "contract-asset-list",
      columnFamily = ContractCF,
      key = bytes(ContractAssetListPrefix, stateId.toByteArray),
      decoder = readTxIds(_).toSet,
      encoder = assets => writeTxIds(assets.toSeq)
    )

  def contractAssetBalanceHistory(stateId: BigInt, assetId: ByteStr): Key[Seq[Int]] =
    historyKey(
      name = "contract-asset-balance-history",
      columnFamily = ContractCF,
      prefix = ContractAssetBalanceHistoryPrefix,
      stateId.toByteArray ++ assetId.arr
    )

  def contractAssetBalance(stateId: BigInt, assetId: ByteStr)(height: Int): Key[Long] =
    Key(
      name = "contract-asset-balance",
      columnFamily = ContractCF,
      key = hBytes(ContractAssetBalancePrefix, height, stateId.toByteArray ++ assetId.arr),
      decoder = Option(_).fold(0L)(Longs.fromByteArray),
      encoder = Longs.toByteArray
    )

  val LastContractStateId: Key[Option[BigInt]] =
    Key.opt("last-contract-state-id", ContractCF, bytes(LastContractStateIdPrefix, Array.emptyByteArray), BigInt(_), _.toByteArray)

  def changedContracts(height: Int): Key[Seq[BigInt]] =
    Key("changed-contracts", h(ChangedContractsPrefix, height), readBigIntSeq, writeBigIntSeq)
}
