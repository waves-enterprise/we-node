package com.wavesenterprise.database.keys

import com.google.common.primitives.{Longs, Shorts}
import com.wavesenterprise.database.KeyHelpers._
import com.wavesenterprise.database._
import com.wavesenterprise.database.RocksDBSet._
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.ContractCF
import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.{ByteStr, DataEntry, LeaseBalance}
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
  val ContractLeaseBalanceHistoryPrefix: Short = 21
  val ContractLeaseBalancePrefix: Short        = 22

  def contractIdsSet(storage: MainRocksDBStorage): MainRocksDBSet[ByteStr] =
    RocksDBSet.newMain[ByteStr](
      name = "contract-ids",
      columnFamily = ContractCF,
      prefix = Shorts.toByteArray(ContractIdsPrefix),
      storage = storage,
      itemEncoder = (_: ByteStr).arr,
      itemDecoder = ByteStr(_)
    )

  def contractHistory(contractId: ByteStr): MainDBKey[Seq[Int]] = historyKey("contract-history", ContractCF, ContractHistoryPrefix, contractId.arr)

  def contract(contractId: ByteStr)(height: Int): MainDBKey[Option[ContractInfo]] =
    MainDBKey.opt("contract", ContractCF, hBytes(ContractPrefix, height, contractId.arr), readContractInfo, writeContractInfo)

  def contractDataHistory(contractId: ByteStr, key: String): MainDBKey[Seq[Int]] =
    historyKey("contract-data-history", ContractCF, ContractDataHistoryPrefix, contractId.arr ++ key.getBytes(UTF_8))

  def contractData(contractId: ByteStr, key: String)(height: Int): MainDBKey[Option[DataEntry[_]]] =
    MainDBKey.opt(
      "contract-data",
      ContractCF,
      hBytes(ContractDataPrefix, height, contractId.arr ++ key.getBytes(UTF_8)),
      ContractTransactionEntryOps.parseValue(key, _, 0)._1,
      ContractTransactionEntryOps.valueBytes
    )

  def executedTxIdFor(txId: ByteStr): MainDBKey[Option[ByteStr]] =
    MainDBKey.opt("executed-tx-id-for", ContractCF, hash(ExecutedTxMappingPrefix, txId), ByteStr(_), _.arr)

  def contractKeys(contractId: ByteStr, storage: MainRocksDBStorage): MainRocksDBSet[String] =
    RocksDBSet.newMain(
      name = "contract-keys",
      columnFamily = ContractCF,
      prefix = bytes(ContractKeysPrefix, contractId.arr),
      storage = storage,
      itemEncoder = _.getBytes(UTF_8),
      itemDecoder = new String(_, UTF_8)
    )

  def contractIdToStateId(contractId: ByteStr): MainDBKey[Option[BigInt]] =
    MainDBKey.opt(
      name = "contract-id-to-state-id",
      columnFamily = ContractCF,
      key = bytes(ContractIdStateIdPrefix, contractId.arr),
      parser = BigInt(_),
      encoder = _.toByteArray
    )

  def stateIdToContractId(stateId: BigInt): MainDBKey[ByteStr] =
    MainDBKey(
      name = "state-id-to-contract-id",
      columnFamily = ContractCF,
      key = bytes(ContractStateIdIdPrefix, stateId.toByteArray),
      decoder = ByteStr(_),
      encoder = _.arr
    )

  def contractWestBalanceHistory(stateId: BigInt): MainDBKey[Seq[Int]] =
    historyKey(
      name = "contract-west-balance-history",
      columnFamily = ContractCF,
      prefix = ContractWestBalanceHistoryPrefix,
      stateId.toByteArray
    )

  def contractWestBalance(stateId: BigInt)(height: Int): MainDBKey[Long] =
    MainDBKey(
      name = "contract-west-balance",
      columnFamily = ContractCF,
      key = hBytes(ContractWestBalancePrefix, height, stateId.toByteArray),
      decoder = Option(_).fold(0L)(Longs.fromByteArray),
      encoder = Longs.toByteArray
    )

  def contractAssetList(stateId: BigInt): MainDBKey[Set[ByteStr]] =
    MainDBKey(
      name = "contract-asset-list",
      columnFamily = ContractCF,
      key = bytes(ContractAssetListPrefix, stateId.toByteArray),
      decoder = readTxIds(_).toSet,
      encoder = assets => writeTxIds(assets.toSeq)
    )

  def contractAssetBalanceHistory(stateId: BigInt, assetId: ByteStr): MainDBKey[Seq[Int]] =
    historyKey(
      name = "contract-asset-balance-history",
      columnFamily = ContractCF,
      prefix = ContractAssetBalanceHistoryPrefix,
      stateId.toByteArray ++ assetId.arr
    )

  def contractAssetBalance(stateId: BigInt, assetId: ByteStr)(height: Int): MainDBKey[Long] =
    MainDBKey(
      name = "contract-asset-balance",
      columnFamily = ContractCF,
      key = hBytes(ContractAssetBalancePrefix, height, stateId.toByteArray ++ assetId.arr),
      decoder = Option(_).fold(0L)(Longs.fromByteArray),
      encoder = Longs.toByteArray
    )

  val LastContractStateId: MainDBKey[Option[BigInt]] =
    MainDBKey.opt("last-contract-state-id", ContractCF, bytes(LastContractStateIdPrefix, Array.emptyByteArray), BigInt(_), _.toByteArray)
  def contractLeaseBalanceHistory(stateId: BigInt): MainDBKey[Seq[Int]] =
    historyKey("contract-lease-balance-history", ContractLeaseBalanceHistoryPrefix, stateId.toByteArray)

  def contractLeaseBalance(stateId: BigInt)(height: Int): MainDBKey[LeaseBalance] =
    MainDBKey("lease-balance", hAddr(ContractLeaseBalancePrefix, height, stateId), readLeaseBalance, writeLeaseBalance)

  def changedContracts(height: Int): MainDBKey[Seq[BigInt]] =
    MainDBKey("changed-contracts", h(ChangedContractsPrefix, height), readBigIntSeq, writeBigIntSeq)
}
