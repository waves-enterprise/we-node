package com.wavesenterprise.database

import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.primitives.{Ints, Longs, Shorts}
import com.wavesenterprise.account.{Address, Alias}
import com.wavesenterprise.block.BlockHeader
import com.wavesenterprise.database.keys.{AddressCFKeys, BlockCFKeys, TransactionCFKeys}
import com.wavesenterprise.database.rocksdb.RocksDBStorage
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.smart.script.{Script, ScriptReader}
import com.wavesenterprise.transaction.{DataTransactionEntryOps, Transaction}

object Keys {
  import KeyHelpers._

  private[database] val PrefixLength: Int = 2

  private[database] val SchemaVersionPrefix: Short             = 0
  private[database] val HeightPrefix: Short                    = 1
  private[database] val WestBalanceHistoryPrefix: Short        = 5
  private[database] val WestBalancePrefix: Short               = 6
  private[database] val AssetListPrefix: Short                 = 7
  private[database] val AssetBalanceHistoryPrefix: Short       = 8
  private[database] val AssetBalancePrefix: Short              = 9
  private[database] val AssetInfoHistoryPrefix: Short          = 10
  private[database] val AssetInfoPrefix: Short                 = 11
  private[database] val LeaseBalanceHistoryPrefix: Short       = 12
  private[database] val LeaseBalancePrefix: Short              = 13
  private[database] val LeaseStatusHistoryPrefix: Short        = 14
  private[database] val LeaseStatusPrefix: Short               = 15
  private[database] val FilledVolumeAndFeeHistoryPrefix: Short = 16
  private[database] val FilledVolumeAndFeePrefix: Short        = 17
  private[database] val ChangedAddressesPrefix: Short          = 21
  private[database] val AddressScriptHistoryPrefix: Short      = 27
  private[database] val AddressScriptPrefix: Short             = 28
  private[database] val ApprovedFeaturesPrefix: Short          = 29
  private[database] val ActivatedFeaturesPrefix: Short         = 30
  private[database] val DataHistoryPrefix: Short               = 33
  private[database] val DataPrefix: Short                      = 34
  private[database] val SponsorshipHistoryPrefix: Short        = 35
  private[database] val SponsorshipPrefix: Short               = 36
  private[database] val AddressesForWestSeqPrefix: Short       = 37
  private[database] val AddressesForWestPrefix: Short          = 38
  private[database] val AddressesForAssetSeqPrefix: Short      = 39
  private[database] val AddressesForAssetPrefix: Short         = 40
  private[database] val AddressTransactionSeqPrefix: Short     = 41
  private[database] val AddressTransactionIdsPrefix: Short     = 42
  private[database] val CarryFeeHistoryPrefix: Short           = 44
  private[database] val CarryFeePrefix: Short                  = 45
  private[database] val AssetScriptHistoryPrefix: Short        = 46
  private[database] val AssetScriptPrefix: Short               = 47
  private[database] val SafeRollbackHeightPrefix: Short        = 48
  private[database] val AssetIdsPrefix: Short                  = 49
  private[database] val DataKeysPrefix: Short                  = 50

  val schemaVersion: Key[Option[Int]] = Key.opt("schema-version", Shorts.toByteArray(SchemaVersionPrefix), Ints.fromByteArray, Ints.toByteArray)

  val height: Key[Int] = intKey("height", HeightPrefix)

  def score(height: Int): Key[BigInt] = BlockCFKeys.score(height)

  def blockHeaderAndSizeAt(height: Int): Key[Option[(BlockHeader, Int)]] = BlockCFKeys.blockHeaderAndSizeAt(height)

  def blockHeaderBytesAt(height: Int): Key[Option[Array[Byte]]] = BlockCFKeys.blockHeaderBytesAt(height)

  def heightOf(blockId: ByteStr): Key[Option[Int]] = BlockCFKeys.heightOf(blockId)

  def westBalanceHistory(addressId: BigInt): Key[Seq[Int]] = historyKey("west-balance-history", WestBalanceHistoryPrefix, addressId.toByteArray)
  def westBalance(addressId: BigInt)(height: Int): Key[Long] =
    Key("west-balance", hAddr(WestBalancePrefix, height, addressId), Option(_).fold(0L)(Longs.fromByteArray), Longs.toByteArray)

  def assetList(addressId: BigInt): Key[Set[ByteStr]] =
    Key("asset-list", addr(AssetListPrefix, addressId), readTxIds(_).toSet, assets => writeTxIds(assets.toSeq))
  def assetBalanceHistory(addressId: BigInt, assetId: ByteStr): Key[Seq[Int]] =
    historyKey("asset-balance-history", AssetBalanceHistoryPrefix, addressId.toByteArray ++ assetId.arr)
  def assetBalance(addressId: BigInt, assetId: ByteStr)(height: Int): Key[Long] =
    Key("asset-balance",
        hBytes(AssetBalancePrefix, height, addressId.toByteArray ++ assetId.arr),
        Option(_).fold(0L)(Longs.fromByteArray),
        Longs.toByteArray)

  def assetInfoHistory(assetId: ByteStr): Key[Seq[Int]] = historyKey("asset-info-history", AssetInfoHistoryPrefix, assetId.arr)
  def assetInfo(assetId: ByteStr)(height: Int): Key[AssetInfo] =
    Key("asset-info", hBytes(AssetInfoPrefix, height, assetId.arr), readAssetInfo, writeAssetInfo)

  def leaseBalanceHistory(addressId: BigInt): Key[Seq[Int]] = historyKey("lease-balance-history", LeaseBalanceHistoryPrefix, addressId.toByteArray)
  def leaseBalance(addressId: BigInt)(height: Int): Key[LeaseBalance] =
    Key("lease-balance", hAddr(LeaseBalancePrefix, height, addressId), readLeaseBalance, writeLeaseBalance)
  def leaseStatusHistory(leaseId: ByteStr): Key[Seq[Int]] = historyKey("lease-status-history", LeaseStatusHistoryPrefix, leaseId.arr)
  def leaseStatus(leaseId: ByteStr)(height: Int): Key[Boolean] =
    Key("lease-status", hBytes(LeaseStatusPrefix, height, leaseId.arr), _(0) == 1, active => Array[Byte](if (active) 1 else 0))

  def filledVolumeAndFeeHistory(orderId: ByteStr): Key[Seq[Int]] =
    historyKey("filled-volume-and-fee-history", FilledVolumeAndFeeHistoryPrefix, orderId.arr)
  def filledVolumeAndFee(orderId: ByteStr)(height: Int): Key[VolumeAndFee] =
    Key("filled-volume-and-fee", hBytes(FilledVolumeAndFeePrefix, height, orderId.arr), readVolumeAndFee, writeVolumeAndFee)

  def transactionInfo(txId: ByteStr): Key[Option[(Int, Transaction)]] = TransactionCFKeys.transactionInfo(txId)
  def transactionBytes(txId: ByteStr): Key[Option[Array[Byte]]]       = TransactionCFKeys.transactionBytes(txId)

  def changedAddresses(height: Int): Key[Seq[BigInt]] = Key("changed-addresses", h(ChangedAddressesPrefix, height), readBigIntSeq, writeBigIntSeq)

  def transactionIdsAtHeight(height: Int): Key[Seq[ByteStr]] = BlockCFKeys.transactionIdsAtHeight(height)

  def addressIdOfAlias(alias: Alias): Key[Option[BigInt]] = AddressCFKeys.addressIdOfAlias(alias)

  def issuedAliasesByAddressId(addressId: BigInt, storage: RocksDBStorage): RocksDBSet[Alias] =
    AddressCFKeys.issuedAliasesByAddressId(addressId, storage)

  val lastAddressId: Key[Option[BigInt]] = AddressCFKeys.LastAddressId

  def addressId(address: Address): Key[Option[BigInt]] = AddressCFKeys.addressId(address)

  def idToAddress(id: BigInt): Key[Address] = AddressCFKeys.idToAddress(id)

  def addressScriptHistory(addressId: BigInt): Key[Seq[Int]] = historyKey("address-script-history", AddressScriptHistoryPrefix, addressId.toByteArray)
  def addressScript(addressId: BigInt)(height: Int): Key[Option[Script]] =
    Key.opt("address-script", hAddr(AddressScriptPrefix, height, addressId), ScriptReader.fromBytes(_).explicitGet(), _.bytes().arr)

  val approvedFeatures: Key[Map[Short, Int]] = Key("approved-features", Shorts.toByteArray(ApprovedFeaturesPrefix), readFeatureMap, writeFeatureMap)
  val activatedFeatures: Key[Map[Short, Int]] =
    Key("activated-features", Shorts.toByteArray(ActivatedFeaturesPrefix), readFeatureMap, writeFeatureMap)

  def dataHistory(addressId: BigInt, key: String): Key[Seq[Int]] =
    historyKey("data-history", DataHistoryPrefix, addressId.toByteArray ++ key.getBytes(UTF_8))
  def data(addressId: BigInt, key: String)(height: Int): Key[Option[DataEntry[_]]] =
    Key.opt(
      "data",
      hBytes(DataPrefix, height, addressId.toByteArray ++ key.getBytes(UTF_8)),
      DataTransactionEntryOps.parseValue(key, _, 0)._1,
      DataTransactionEntryOps.valueBytes
    )

  def sponsorshipHistory(assetId: ByteStr): Key[Seq[Int]] = historyKey("sponsorship-history", SponsorshipHistoryPrefix, assetId.arr)
  def sponsorship(assetId: ByteStr)(height: Int): Key[SponsorshipValue] =
    Key("sponsorship", hBytes(SponsorshipPrefix, height, assetId.arr), readSponsorship, writeSponsorship)

  val addressesForWestSeqNr: Key[Int]                = intKey("addresses-for-west-seq-nr", AddressesForWestSeqPrefix)
  def addressesForWest(seqNr: Int): Key[Seq[BigInt]] = Key("addresses-for-west", h(AddressesForWestPrefix, seqNr), readBigIntSeq, writeBigIntSeq)

  def addressesForAssetSeqNr(assetId: ByteStr): Key[Int] = bytesSeqNr("addresses-for-asset-seq-nr", AddressesForAssetSeqPrefix, assetId.arr)
  def addressesForAsset(assetId: ByteStr, seqNr: Int): Key[Seq[BigInt]] =
    Key("addresses-for-asset", hBytes(AddressesForAssetPrefix, seqNr, assetId.arr), readBigIntSeq, writeBigIntSeq)

  def addressTransactionSeqNr(addressId: BigInt): Key[Int] =
    bytesSeqNr("address-transaction-seq-nr", AddressTransactionSeqPrefix, addressId.toByteArray)
  def addressTransactionIds(addressId: BigInt, seqNr: Int): Key[Seq[(Int, ByteStr)]] =
    Key("address-transaction-ids", hBytes(AddressTransactionIdsPrefix, seqNr, addressId.toByteArray), readTransactionIds, writeTransactionIds)

  val carryFeeHistory: Key[Seq[Int]]   = historyKey("carry-fee-history", CarryFeeHistoryPrefix, Array())
  def carryFee(height: Int): Key[Long] = Key("carry-fee", h(CarryFeePrefix, height), Option(_).fold(0L)(Longs.fromByteArray), Longs.toByteArray)

  def assetScriptHistory(assetId: ByteStr): Key[Seq[Int]] = historyKey("asset-script-history", AssetScriptHistoryPrefix, assetId.arr)
  def assetScript(assetId: ByteStr)(height: Int): Key[Option[Script]] =
    Key.opt("asset-script", hBytes(AssetScriptPrefix, height, assetId.arr), ScriptReader.fromBytes(_).explicitGet(), _.bytes().arr)

  def assetScriptPresent(assetId: ByteStr)(height: Int): Key[Option[Unit]] =
    Key.opt("asset-script", hBytes(AssetScriptPrefix, height, assetId.arr), _ => (), _ => Array[Byte]())

  val safeRollbackHeight: Key[Int] = intKey("safe-rollback-height", SafeRollbackHeightPrefix)

  def blockTransactionsAtHeight(height: Int): Key[Seq[ByteStr]] = BlockCFKeys.blockTransactionsAtHeight(height)

  def assetIdsSet(storage: RocksDBStorage): RocksDBSet[ByteStr] =
    new RocksDBSet[ByteStr](
      name = "asset-ids",
      prefix = Shorts.toByteArray(AssetIdsPrefix),
      storage = storage,
      itemEncoder = (_: ByteStr).arr,
      itemDecoder = ByteStr(_)
    )

  def dataKeys(addressId: BigInt, storage: RocksDBStorage): RocksDBSet[String] =
    new RocksDBSet[String](
      name = "data-keys",
      prefix = addr(DataKeysPrefix, addressId),
      storage = storage,
      itemEncoder = _.getBytes(UTF_8),
      itemDecoder = new String(_, UTF_8)
    )
}
