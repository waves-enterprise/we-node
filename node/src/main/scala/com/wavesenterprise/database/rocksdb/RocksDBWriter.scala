package com.wavesenterprise.database.rocksdb

import cats.implicits._
import com.google.common.cache.CacheBuilder
import com.google.common.io.ByteStreams.newDataOutput
import com.google.common.primitives.{Longs, Shorts}
import com.wavesenterprise.account.{Address, AddressOrAlias, Alias, PublicKeyAccount}
import com.wavesenterprise.acl.{OpType, PermissionOp, Permissions, Role}
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block._
import com.wavesenterprise.consensus.MinerBanlistEntry.CancelledWarning
import com.wavesenterprise.consensus._
import com.wavesenterprise.crypto.PublicKey
import com.wavesenterprise.database._
import com.wavesenterprise.database.address.AddressTransactions
import com.wavesenterprise.database.certs.CertificatesWriter
import com.wavesenterprise.database.keys.CertificatesCFKeys.CrlByKeyPrefix
import com.wavesenterprise.database.keys.CrlKey
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.CertsCF
import com.wavesenterprise.database.docker.{KeysPagination, KeysRequest}
import com.wavesenterprise.database.keys.{ContractCFKeys, LeaseCFKeys}
import com.wavesenterprise.database.migration.MainnetMigration
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.privacy._
import com.wavesenterprise.settings.{ConsensusSettings, FunctionalitySettings, WESettings}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state._
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.transaction.ValidationError.{AliasDoesNotExist, GenericError}
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.acl.PermitTransaction
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.assets.exchange.ExchangeTransaction
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation.{
  ContractBurnV1,
  ContractCancelLeaseV1,
  ContractIssueV1,
  ContractLeaseV1,
  ContractReissueV1,
  ContractTransferOutV1
}
import com.wavesenterprise.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import com.wavesenterprise.transaction.smart.SetScriptTransaction
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.transfer.{MassTransferTransaction, TransferTransaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.pki.CrlData
import com.wavesenterprise.utils.{Paged, ScorexLogging}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils
import org.rocksdb.RocksIterator

import java.net.URL
import java.security.cert.{Certificate, X509CRL, X509Certificate}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}

object RocksDBWriter {

  def apply(storage: MainRocksDBStorage, settings: WESettings): RocksDBWriter = {
    new RocksDBWriter(
      storage,
      settings.blockchain.custom.functionality,
      settings.blockchain.consensus,
      settings.additionalCache.rocksdb.maxCacheSize,
      settings.additionalCache.rocksdb.maxRollbackDepth,
    )
  }

  /** {{{
    * ([10, 7, 4], 5, 11) => [10, 7, 4]
    * ([10, 7], 5, 11) => [10, 7, 1]
    * }}}
    */
  private[database] def slice(v: Seq[Int], from: Int, to: Int): Seq[Int] = {
    val (c1, c2) = v.dropWhile(_ > to).partition(_ > from)
    c1 :+ c2.headOption.getOrElse(1)
  }

  implicit class ReadOnlyDBExt(override val db: MainReadOnlyDB) extends BaseReadOnlyDBExt[MainDBColumnFamily, MainReadOnlyDB](db)

  abstract class BaseReadOnlyDBExt[CF <: ColumnFamily, RO <: BaseReadOnlyDB[CF]](val db: RO) {
    type Key[V] = BaseKey[V, CF]

    def fromHistory[A](historyKey: Key[Seq[Int]], valueKey: Int => Key[A]): Option[A] =
      for {
        lastChange <- db.get(historyKey).headOption
      } yield db.get(valueKey(lastChange))

    def hasInHistory(historyKey: Key[Seq[Int]], v: Int => Key[_]): Boolean =
      db.get(historyKey)
        .headOption
        .exists(h => db.has(v(h)))
  }

  implicit class RWExt(val db: MainReadWriteDB) extends AnyVal {
    def fromHistory[A](historyKey: MainDBKey[Seq[Int]], valueKey: Int => MainDBKey[A]): Option[A] =
      for {
        lastChange <- db.get(historyKey).headOption
      } yield db.get(valueKey(lastChange))
  }

  case class LeaseParticipantsInfo(leaseId: LeaseId, sender: AssetHolder, recipient: Address)

}

trait ReadWriteDB {
  protected def storage: MainRocksDBStorage
  protected[database] def readOnly[A](f: MainReadOnlyDB => A): A = storage.readOnly(f)

  /**
    * Async unsafe method!
    * There is no locks inside, do not fall in trap cause of his name.
    */
  protected def readWrite[A](f: MainReadWriteDB => A): A = storage.readWrite(f)
}

class RocksDBWriter(val storage: MainRocksDBStorage,
                    val fs: FunctionalitySettings,
                    val consensus: ConsensusSettings,
                    val maxCacheSize: Int,
                    val maxRollbackDepth: Int)
    extends ReadWriteDB
    with PrivacyState
    with CertificatesWriter
    with Caches
    with HistoryUpdater[MainDBColumnFamily, MainReadOnlyDB, MainReadWriteDB]
    with CollectKeysToDiscardOps
    with ScorexLogging {

  private val balanceSnapshotMaxRollbackDepth: Int = maxRollbackDepth + 1000

  import RocksDBWriter._

  /**
    * Shouldn't ever be called for PoS Consensus
    */
  private def maybeBanHistoryBuilder(targetHeight: Int): Option[MinerBanHistoryBuilder] = {
    def selectBuilder(banDuration: Int, warningsForBan: Int) =
      if (activatedFeatures.get(BlockchainFeature.MinerBanHistoryOptimisationFix.id).exists(_ <= targetHeight)) {
        Some(new MinerBanHistoryBuilderV2(banDuration, warningsForBan))
      } else {
        implicit val minerBanHistoryOrdering: Ordering[MinerBanlistEntry] = MinerBanHistory.selectOrdering(activatedFeatures, height)
        Some(new MinerBanHistoryBuilderV1(banDuration, warningsForBan))
      }

    consensus match {
      case ConsensusSettings.PoASettings(_, _, banDuration, warningsForBan, _) =>
        selectBuilder(banDuration, warningsForBan)
      case ConsensusSettings.CftSettings(_, _, banDuration, warningsForBan, _, _, _, _) =>
        selectBuilder(banDuration, warningsForBan)
      case _ =>
        None
    }
  }

  override protected def loadMaxAddressId(): BigInt       = readOnly(db => db.get(Keys.lastAddressId).getOrElse(BigInt(0)))
  override protected def loadMaxContractStateId(): BigInt = readOnly(db => db.get(WEKeys.lastContractStateId).getOrElse(BigInt(0)))

  override protected def loadStateIdByContractId(contractId: ContractId): Option[BigInt] = storage.get(WEKeys.contractIdToStateId(contractId.byteStr))
  override protected def loadContractByStateId(contractStateId: BigInt): ByteStr         = storage.get(WEKeys.stateIdToContractId(contractStateId))

  override protected def loadAddressId(address: Address): Option[BigInt] = storage.get(Keys.addressId(address))
  override protected def loadAddressById(id: BigInt): Address            = storage.get(Keys.idToAddress(id))

  override protected def loadMaxNonEmptyRoleAddressId(): BigInt = readOnly(db => db.get(WEKeys.lastNonEmptyRoleAddressId).getOrElse(BigInt(0)))

  override protected def loadNonEmptyRoleAddressId(address: Address): Option[BigInt] = readOnly(db => db.get(WEKeys.nonEmptyRoleAddressId(address)))

  override protected def loadHeight(): Int = readOnly(_.get(Keys.height))

  override protected def safeRollbackHeight: Int = readOnly(_.get(Keys.safeRollbackHeight))

  override protected def loadScore(): BigInt = readOnly(db => db.get(Keys.score(db.get(Keys.height))))

  override protected def loadLastBlock(): Option[Block] = readOnly { db =>
    loadBlock(db.get(Keys.height), db)
  }

  override protected def loadScript(address: Address): Option[Script] = readOnly { db =>
    addressId(address).fold(Option.empty[Script]) { addressId =>
      db.fromHistory(Keys.addressScriptHistory(addressId), Keys.addressScript(addressId)).flatten
    }
  }

  override protected def hasScriptBytes(address: Address): Boolean = readOnly { db =>
    addressId(address).fold(false) { addressId =>
      db.hasInHistory(Keys.addressScriptHistory(addressId), Keys.addressScript(addressId))
    }
  }

  override protected def loadAssetScript(asset: AssetId): Option[Script] = readOnly { db =>
    db.fromHistory(Keys.assetScriptHistory(asset), Keys.assetScript(asset)).flatten
  }

  override protected def hasAssetScriptBytes(asset: AssetId): Boolean = readOnly { db =>
    db.fromHistory(Keys.assetScriptHistory(asset), Keys.assetScriptPresent(asset)).flatten.nonEmpty
  }

  override def carryFee: Long = readOnly(_.get(Keys.carryFee(height)))

  def accountDataSlice(address: Address, from: Int, to: Int): AccountDataInfo = readOnly { db =>
    addressId(address) match {
      case None => AccountDataInfo(Map.empty)
      case Some(addressId) =>
        val keys       = Keys.dataKeys(addressId, storage).members(db)
        val pagination = new KeysPagination(keys.iterator)
        val values = pagination
          .paginatedKeys(Some(from), Some(to - from), None)
          .flatMap { key =>
            accountData(address, key)
          }
          .map(e => e.key -> e)

        AccountDataInfo(values.toMap)
    }
  }

  override def accountData(address: Address): AccountDataInfo = readOnly { db =>
    AccountDataInfo((for {
      addressId <- addressId(address).toSeq
      key       <- Keys.dataKeys(addressId, storage).members(db)
      value     <- accountData(address, key)
    } yield key -> value).toMap)
  }

  override def accountData(address: Address, key: String): Option[DataEntry[_]] = readOnly { db =>
    addressId(address).fold(Option.empty[DataEntry[_]]) { addressId =>
      db.fromHistory(Keys.dataHistory(addressId, key), Keys.data(addressId, key)).flatten
    }
  }

  override def addressBalance(address: Address, mayBeAssetId: Option[AssetId]): Long = readOnly { db =>
    addressId(address).fold(0L) { addressId =>
      mayBeAssetId match {
        case Some(assetId) => db.fromHistory(Keys.assetBalanceHistory(addressId, assetId), Keys.assetBalance(addressId, assetId)).getOrElse(0L)
        case None          => db.fromHistory(Keys.westBalanceHistory(addressId), Keys.westBalance(addressId)).getOrElse(0L)
      }
    }
  }

  override def contractBalance(contractId: ContractId, mayBeAssetId: Option[AssetId], readingContext: ContractReadingContext): Long = readOnly { db =>
    stateIdByContractId(contractId).fold(0L) { contractStateId =>
      mayBeAssetId match {
        case Some(assetId) =>
          db.fromHistory(WEKeys.contractAssetBalanceHistory(contractStateId, assetId), WEKeys.contractAssetBalance(contractStateId, assetId))
            .getOrElse(0L)
        case None => db.fromHistory(WEKeys.contractWestBalanceHistory(contractStateId), WEKeys.contractWestBalance(contractStateId)).getOrElse(0L)
      }
    }
  }

  override def addressWestDistribution(height: Int): Map[Address, Long] = readOnly { db =>
    (for {
      seqNr     <- (1 to db.get(Keys.addressesForWestSeqNr)).par
      addressId <- db.get(Keys.addressesForWest(seqNr)).par
      history = db.get(Keys.westBalanceHistory(addressId))
      actualHeight <- history.partition(_ > height)._2.headOption
      balance = db.get(Keys.westBalance(addressId)(actualHeight))
      if balance > 0
    } yield db.get(Keys.idToAddress(addressId)) -> balance).toMap.seq
  }

  private def loadAddressLeaseBalanceInternal(db: MainReadOnlyDB, addressId: BigInt): LeaseBalance = {
    val lease = db.fromHistory(Keys.leaseBalanceHistory(addressId), Keys.leaseBalance(addressId)).getOrElse(LeaseBalance.empty)
    lease
  }

  override protected def loadAddressLeaseBalance(address: Address): LeaseBalance = readOnly { db =>
    addressId(address).fold(LeaseBalance.empty)(loadAddressLeaseBalanceInternal(db, _))
  }

  private def loadContractLeaseBalanceInternal(db: MainReadOnlyDB, contractId: BigInt): LeaseBalance = {
    val lease = db.fromHistory(ContractCFKeys.contractLeaseBalanceHistory(contractId), ContractCFKeys.contractLeaseBalance(contractId)).getOrElse(
      LeaseBalance.empty)
    lease
  }

  override protected def loadContractLeaseBalance(contractId: ContractId): LeaseBalance = readOnly { db =>
    stateIdByContractId(contractId).fold(LeaseBalance.empty)(loadContractLeaseBalanceInternal(db, _))
  }

  private def loadLposAddressPortfolio(db: MainReadOnlyDB, addressId: BigInt) = Portfolio(
    db.fromHistory(Keys.westBalanceHistory(addressId), Keys.westBalance(addressId)).getOrElse(0L),
    loadAddressLeaseBalanceInternal(db, addressId),
    Map.empty
  )

  private def loadPortfolio(db: MainReadOnlyDB, addressId: BigInt) = loadLposAddressPortfolio(db, addressId).copy(
    assets = (for {
      assetId <- db.get(Keys.assetList(addressId))
    } yield assetId -> db.fromHistory(Keys.assetBalanceHistory(addressId, assetId), Keys.assetBalance(addressId, assetId)).getOrElse(0L)).toMap
  )

  override protected def loadAddressPortfolio(address: Address): Portfolio = readOnly { db =>
    addressId(address).fold(Portfolio.empty)(loadPortfolio(db, _))
  }

  private def loadLposContractPortfolio(db: MainReadOnlyDB, contractStateId: BigInt) = Portfolio(
    db.fromHistory(WEKeys.contractWestBalanceHistory(contractStateId), WEKeys.contractWestBalance(contractStateId)).getOrElse(0L),
    LeaseBalance.empty,
    Map.empty
  )

  private def loadContractPortfolio(db: MainReadOnlyDB, contractStateId: BigInt) = loadLposContractPortfolio(db, contractStateId).copy(
    assets = (for {
      assetId <- db.get(WEKeys.contractAssetList(contractStateId))
    } yield assetId -> db
      .fromHistory(WEKeys.contractAssetBalanceHistory(contractStateId, assetId), WEKeys.contractAssetBalance(contractStateId, assetId))
      .getOrElse(0L)).toMap
  )

  override protected def loadContractPortfolio(contractId: ContractId): Portfolio = readOnly { db =>
    loadStateIdByContractId(contractId).fold(Portfolio.empty)(loadContractPortfolio(db, _))
  }

  override def assets(): Set[AssetId] = assetIdsSet.members

  override def asset(id: AssetId): Option[AssetInfo] = readOnly { db =>
    db.fromHistory(Keys.assetInfoHistory(id), Keys.assetInfo(id))
  }

  override protected def loadAssetDescription(assetId: ByteStr): Option[AssetDescription] = readOnly { db =>
    db.fromHistory(Keys.assetInfoHistory(assetId), Keys.assetInfo(assetId)).map { ai =>
      val sponsorshipIsEnabled = db.fromHistory(Keys.sponsorshipHistory(assetId), Keys.sponsorship(assetId)).fold(false)(_.isEnabled)
      val script               = db.fromHistory(Keys.assetScriptHistory(assetId), Keys.assetScript(assetId)).flatten
      AssetDescription(ai, script, sponsorshipIsEnabled)
    }
  }

  override protected def loadVolumeAndFee(orderId: ByteStr): VolumeAndFee = readOnly { db =>
    db.fromHistory(Keys.filledVolumeAndFeeHistory(orderId), Keys.filledVolumeAndFee(orderId)).getOrElse(VolumeAndFee.empty)
  }

  override protected def loadApprovedFeatures(): Map[Short, Int] = readOnly(_.get(Keys.approvedFeatures))

  override protected def loadActivatedFeatures(): Map[Short, Int] = fs.preActivatedFeatures ++ readOnly(_.get(Keys.activatedFeatures))

  private def updateHistory(rw: MainReadWriteDB,
                            key: MainDBKey[Seq[Int]],
                            threshold: Int,
                            kf: Int => MainDBKey[_]): (MainDBColumnFamily, Seq[Array[Byte]]) =
    updateHistory(rw, rw.get(key), key, threshold, kf)

  private def updateHistory(rw: MainReadWriteDB,
                            history: Seq[Int],
                            key: MainDBKey[Seq[Int]],
                            threshold: Int,
                            kf: Int => MainDBKey[_]): (MainDBColumnFamily, Seq[Array[Byte]]) =
    super.updateHistory(rw, history, key, threshold, kf)(height)

  private[this] val contractIdsSet = WEKeys.contractIdsSet(storage)
  private[this] val assetIdsSet    = Keys.assetIdsSet(storage)
  private[this] val policyIdsSet   = WEKeys.policyIdsSet(storage)

  private[this] val crlIssuersSet = WEKeys.crlIssuers(storage)

  private[this] val mainnetPatch = List(
    3550140 -> ByteStr.decodeBase58("41puwxf6m6RsJiBK5hPuf7uPnSLYYc6iLxY86RHjJsPftw1TvJHS4Eph1Nno9VrtLmioZv56NBQg1AU6FkZgP5yf").get
  )

  // noinspection ScalaStyle
  override protected def doAppend(
      block: Block,
      carryFee: Long,
      newAssetHolders: Map[AssetHolder, BigInt],
      newNonEmptyRoleAddresses: Map[Address, BigInt],
      westAddressesBalances: Map[BigInt, Long],
      assetAddressesBalances: Map[BigInt, Map[ByteStr, Long]],
      addressLeaseBalances: Map[BigInt, LeaseBalance],
      contractLeaseBalances: Map[BigInt, LeaseBalance],
      westContractsBalances: Map[BigInt, Long],
      assetContractsBalances: Map[BigInt, Map[ByteStr, Long]],
      leaseMap: Map[LeaseId, LeaseDetails],
      leaseCancelMap: Map[LeaseId, LeaseDetails],
      transactions: Seq[Transaction],
      addressTransactions: Map[BigInt, List[(Int, ByteStr)]],
      contractTransactions: Map[BigInt, List[(Int, ByteStr)]],
      assets: Map[ByteStr, AssetInfo],
      filledQuantity: Map[ByteStr, VolumeAndFee],
      scripts: Map[BigInt, Option[Script]],
      assetScripts: Map[AssetId, Option[Script]],
      data: Map[BigInt, AccountDataInfo],
      aliases: Map[Alias, BigInt],
      sponsorship: Map[AssetId, Sponsorship],
      permissions: Map[BigInt, Permissions],
      registrations: Map[OpType, Seq[(BigInt, PublicKeyAccount)]],
      updatedMiners: Seq[BigInt],
      updatedValidators: Seq[BigInt],
      contracts: Map[ContractId, ContractInfo],
      contractsData: Map[ByteStr, ExecutedContractData],
      executedTxMapping: Map[ByteStr, ByteStr],
      policies: Map[ByteStr, PolicyDiff],
      policiesDataHashes: Map[ByteStr, Set[PolicyDataHashTransaction]],
      minersBanHistory: Map[Address, MinerBanHistory],
      minersCancelledWarnings: Map[Address, Seq[CancelledWarning]],
      certificates: Set[X509Certificate],
      crlDataByHash: Map[ByteStr, CrlData],
      crlHashesByIssuer: Map[PublicKeyAccount, Set[ByteStr]]
  ): Unit = readWrite { rw =>
    val expiredKeys = new ArrayBuffer[(MainDBColumnFamily, Seq[Array[Byte]])]

    rw.put(Keys.height, height)

    val previousSafeRollbackHeight = rw.get(Keys.safeRollbackHeight)

    if (previousSafeRollbackHeight < (height - maxRollbackDepth)) {
      rw.put(Keys.safeRollbackHeight, height - maxRollbackDepth)
    }

    rw.put(Keys.blockHeaderAndSizeAt(height), Some((block.blockHeader, block.bytes().length)))
    rw.put(Keys.heightOf(block.uniqueId), Some(height))
    val (newBalancedAccountsMap, newBalancedContractsMap) = newAssetHolders.partition(_._1.isInstanceOf[Account])
    val lastAddressId                                     = loadMaxAddressId() + newBalancedAccountsMap.size
    val lastContractStateId                               = loadMaxContractStateId() + newBalancedContractsMap.size
    if (newBalancedAccountsMap.nonEmpty) rw.put(Keys.lastAddressId, Some(lastAddressId))
    if (newBalancedContractsMap.nonEmpty) rw.put(WEKeys.lastContractStateId, Some(lastContractStateId))
    rw.put(Keys.score(height), rw.get(Keys.score(height - 1)) + block.blockScore())

    for ((newAccountAddress, id) <- newBalancedAccountsMap.collectAddresses) {
      rw.put(Keys.addressId(newAccountAddress), Some(id))
      log.trace(s"Accounts: WRITE ${newAccountAddress.address} -> $id")
      rw.put(Keys.idToAddress(id), newAccountAddress)
    }
    log.trace(s"WRITE lastAddressId = $lastAddressId")

    for ((newContractId, id) <- newBalancedContractsMap.collectContractIds) {
      rw.put(WEKeys.contractIdToStateId(newContractId.byteStr), Some(id))
      log.trace(s"Contracts: WRITE ${newContractId.byteStr.base58} -> $id")
      rw.put(WEKeys.stateIdToContractId(id), newContractId.byteStr)
    }
    log.trace(s"WRITE lastContractStateId = $lastContractStateId")

    appendNonEmptyRoleAddresses(newNonEmptyRoleAddresses, rw)

    val threshold        = height - maxRollbackDepth
    val balanceThreshold = height - balanceSnapshotMaxRollbackDepth

    val newAddressesForWest = ArrayBuffer.empty[BigInt]
    val updatedBalanceAddresses = for ((addressId, balance) <- westAddressesBalances) yield {
      val kwbh = Keys.westBalanceHistory(addressId)
      val wbh  = rw.get(kwbh)
      if (wbh.isEmpty) {
        newAddressesForWest += addressId
      }
      rw.put(Keys.westBalance(addressId)(height), balance)
      expiredKeys += updateHistory(rw, wbh, kwbh, balanceThreshold, Keys.westBalance(addressId))
      addressId
    }

    val changedAddresses = addressTransactions.keys ++ updatedBalanceAddresses

    if (newAddressesForWest.nonEmpty) {
      val newSeqNr = rw.get(Keys.addressesForWestSeqNr) + 1
      rw.put(Keys.addressesForWestSeqNr, newSeqNr)
      rw.put(Keys.addressesForWest(newSeqNr), newAddressesForWest)
    }

    val newContractsForWest = ArrayBuffer.empty[BigInt]
    val updatedBalanceContracts = for ((contractStateId, balance) <- westContractsBalances) yield {
      val kwbh = WEKeys.contractWestBalanceHistory(contractStateId)
      val wbh  = rw.get(kwbh)
      if (wbh.isEmpty) {
        newContractsForWest += contractStateId
      }
      rw.put(WEKeys.contractWestBalance(contractStateId)(height), balance)
      expiredKeys += updateHistory(rw, wbh, kwbh, balanceThreshold, WEKeys.contractWestBalance(contractStateId))
      contractStateId
    }

    val changedContracts = contractTransactions.keys ++ updatedBalanceContracts

    for ((addressId, leaseBalance) <- addressLeaseBalances) {
      rw.put(Keys.leaseBalance(addressId)(height), leaseBalance)
      expiredKeys += updateHistory(rw, Keys.leaseBalanceHistory(addressId), balanceThreshold, Keys.leaseBalance(addressId))
    }

    for ((contractStateId, leaseBalance) <- contractLeaseBalances) {
      rw.put(ContractCFKeys.contractLeaseBalance(contractStateId)(height), leaseBalance)
      expiredKeys += updateHistory(rw,
                                   ContractCFKeys.contractLeaseBalanceHistory(contractStateId),
                                   balanceThreshold,
                                   ContractCFKeys.contractLeaseBalance(contractStateId))
    }

    val newAddressesForAsset = mutable.AnyRefMap.empty[ByteStr, Set[BigInt]]
    for ((addressId, assets) <- assetAddressesBalances) {
      val prevAssets = rw.get(Keys.assetList(addressId))
      val newAssets  = assets.keySet.diff(prevAssets)
      for (assetId <- newAssets) {
        newAddressesForAsset += assetId -> (newAddressesForAsset.getOrElse(assetId, Set.empty) + addressId)
      }
      rw.put(Keys.assetList(addressId), prevAssets ++ assets.keySet)
      for ((assetId, balance) <- assets) {
        rw.put(Keys.assetBalance(addressId, assetId)(height), balance)
        expiredKeys += updateHistory(rw, Keys.assetBalanceHistory(addressId, assetId), threshold, Keys.assetBalance(addressId, assetId))
      }
    }

    for ((assetId, newAddressIds) <- newAddressesForAsset) {
      val seqNrKey  = Keys.addressesForAssetSeqNr(assetId)
      val nextSeqNr = rw.get(seqNrKey) + 1
      val key       = Keys.addressesForAsset(assetId, nextSeqNr)

      rw.put(seqNrKey, nextSeqNr)
      rw.put(key, newAddressIds.toSeq)
    }

    val newContractsForAsset = mutable.AnyRefMap.empty[ByteStr, Set[BigInt]]
    for ((contractStateId, assets) <- assetContractsBalances) {
      val prevAssets = rw.get(WEKeys.contractAssetList(contractStateId))
      val newAssets  = assets.keySet.diff(prevAssets)
      for (assetId <- newAssets) {
        newContractsForAsset += assetId -> (newContractsForAsset.getOrElse(assetId, Set.empty) + contractStateId)
      }
      rw.put(WEKeys.contractAssetList(contractStateId), prevAssets ++ assets.keySet)
      for ((assetId, balance) <- assets) {
        rw.put(WEKeys.contractAssetBalance(contractStateId, assetId)(height), balance)
        expiredKeys += updateHistory(rw,
                                     WEKeys.contractAssetBalanceHistory(contractStateId, assetId),
                                     threshold,
                                     WEKeys.contractAssetBalance(contractStateId, assetId))
      }
    }

    rw.put(Keys.changedAddresses(height), changedAddresses.toSeq)
    rw.put(WEKeys.changedContracts(height), changedContracts.toSeq)

    for ((orderId, volumeAndFee) <- filledQuantity) {
      rw.put(Keys.filledVolumeAndFee(orderId)(height), volumeAndFee)
      expiredKeys += updateHistory(rw, Keys.filledVolumeAndFeeHistory(orderId), threshold, Keys.filledVolumeAndFee(orderId))
    }

    if (assets.nonEmpty) {
      assetIdsSet.add(rw, assets.keySet)
    }

    for ((assetId, assetInfo) <- assets) {
      rw.put(Keys.assetInfo(assetId)(height), assetInfo)
      expiredKeys += updateHistory(rw, Keys.assetInfoHistory(assetId), threshold, Keys.assetInfo(assetId))
    }

    for ((addressId, script) <- scripts) {
      expiredKeys += updateHistory(rw, Keys.addressScriptHistory(addressId), threshold, Keys.addressScript(addressId))
      script.foreach(s => rw.put(Keys.addressScript(addressId)(height), Some(s)))
    }

    for ((asset, script) <- assetScripts) {
      expiredKeys += updateHistory(rw, Keys.assetScriptHistory(asset), threshold, Keys.assetScript(asset))
      script.foreach(s => rw.put(Keys.assetScript(asset)(height), Some(s)))
    }

    for ((addressId, addressData) <- data) {
      val dataKeys = Keys.dataKeys(addressId, storage)
      val newKeys = for {
        (key, value) <- addressData.data
        historyKey = Keys.dataHistory(addressId, key)
        _          = rw.put(Keys.data(addressId, key)(height), Some(value))
        _          = expiredKeys += updateHistory(rw, historyKey, threshold, Keys.data(addressId, key))
        isNew      = !dataKeys.contains(rw, key)
        if isNew
      } yield key
      if (newKeys.nonEmpty) {
        dataKeys.add(rw, newKeys)
      }
    }

    for ((addressId, txs) <- addressTransactions) {
      val kk        = Keys.addressTransactionSeqNr(addressId)
      val nextSeqNr = rw.get(kk) + 1
      rw.put(Keys.addressTransactionIds(addressId, nextSeqNr), txs)
      rw.put(kk, nextSeqNr)
    }

    for ((alias, addressId) <- aliases) {
      rw.put(Keys.addressIdOfAlias(alias), Some(addressId))
    }

    aliases.toList
      .groupBy { case (_, addressId) => addressId }
      .mapValues(_.map { case (alias, _) => alias })
      .foreach {
        case (addressId, aliases) =>
          Keys.issuedAliasesByAddressId(addressId, storage).add(aliases)
      }

    val transactionIdsAtHeightBuilder = Vector.newBuilder[ByteStr]
    for (tx <- transactions) {
      rw.put(Keys.transactionInfo(tx.id()), Some((height, tx)))
      transactionIdsAtHeightBuilder += tx.id()
    }
    rw.put(Keys.transactionIdsAtHeight(height), transactionIdsAtHeightBuilder.result())

    rw.put(Keys.blockTransactionsAtHeight(height), block.transactionData.map(_.id()))

    val activationWindowSize = fs.featureCheckBlocksPeriod
    if (height % activationWindowSize == 0) {
      val minVotes = fs.blocksForFeatureActivation
      val newlyApprovedFeatures = featureVotes(height).collect {
        case (featureId, voteCount) if voteCount + (if (block.featureVotes(featureId)) 1 else 0) >= minVotes => featureId -> height
      }

      if (newlyApprovedFeatures.nonEmpty) {
        approvedFeaturesCache = newlyApprovedFeatures ++ rw.get(Keys.approvedFeatures)
        rw.put(Keys.approvedFeatures, approvedFeaturesCache)

        val newActivatedFeatures = newlyApprovedFeatures.mapValues(_ + activationWindowSize) ++ rw.get(Keys.activatedFeatures)
        activatedFeaturesCache = newActivatedFeatures ++ fs.preActivatedFeatures
        rw.put(Keys.activatedFeatures, newActivatedFeatures)
      }
    }

    for ((assetId, sp: SponsorshipValue) <- sponsorship) {
      rw.put(Keys.sponsorship(assetId)(height), sp)
      expiredKeys += updateHistory(rw, Keys.sponsorshipHistory(assetId), threshold, Keys.sponsorship(assetId))
    }

    rw.put(Keys.carryFee(height), carryFee)
    expiredKeys += updateHistory(rw, Keys.carryFeeHistory, threshold, Keys.carryFee)

    for ((targetAddr, perms) <- permissions) {
      rw.put(WEKeys.permissions(targetAddr), Some(perms.toSeq))
    }

    val addedParticipants    = registrations.getOrElse(OpType.Add, Seq.empty)
    val excludedParticipants = registrations.getOrElse(OpType.Remove, Seq.empty)

    val oldParticipants     = rw.get(WEKeys.networkParticipants())
    val addedIds            = addedParticipants.map(_._1)
    val excludedIds         = excludedParticipants.map(_._1)
    val updatedParticipants = oldParticipants.diff(excludedIds).union(addedIds)

    rw.put(WEKeys.networkParticipants(), updatedParticipants)

    addedParticipants.foreach {
      case (addressId, pubKey) =>
        rw.put(WEKeys.participantPubKey(addressId), Some(pubKey))
    }

    excludedParticipants.foreach {
      case (addressId, _) =>
        rw.delete(WEKeys.participantPubKey(addressId))
    }

    rw.put(WEKeys.miners, updatedMiners)
    rw.put(WEKeys.validators, updatedValidators)

    if (contracts.nonEmpty) {
      contractIdsSet.add(rw, contracts.keySet.map(_.byteStr))
    }

    for ((contractId, contractInfo) <- contracts) {
      rw.put(WEKeys.contract(contractId.byteStr)(height), Some(contractInfo))
      expiredKeys += updateHistory(rw, WEKeys.contractHistory(contractId.byteStr), threshold, WEKeys.contract(contractId.byteStr))
    }

    for ((contractId, contractData) <- contractsData) {
      val contractKeys = WEKeys.contractKeys(contractId, storage)
      val newKeys = for {
        (key, value) <- contractData.data
        historyKey = WEKeys.contractDataHistory(contractId, key)
        _          = rw.put(WEKeys.contractData(contractId, key)(height), Some(value))
        _          = expiredKeys += updateHistory(rw, historyKey, threshold, WEKeys.contractData(contractId, key))
        isNew      = !contractKeys.contains(rw, key)
        if isNew
      } yield key
      if (newKeys.nonEmpty) {
        contractKeys.add(rw, newKeys)
      }
    }

    for ((txId, executedTxId) <- executedTxMapping) {
      rw.put(WEKeys.executedTxIdFor(txId), Some(executedTxId))
    }

    if (policies.nonEmpty) {
      policyIdsSet.add(rw, policies.keySet)
    }

    for ((policyId, policyDiff: PolicyDiffValue) <- policies) {
      updatePolicy(rw, policyId, policyDiff)
    }

    for ((policyId, dataHashes) <- policiesDataHashes) {
      updatePolicyDataHashes(rw, policyId, dataHashes, OpType.Add)
    }

    expiredKeys.foreach {
      case (columnFamily, keys) => rw.delete(columnFamily, keys)
    }

    val newCerts = certificates.filter(cert => certByDistinguishedName(cert.getSubjectX500Principal.getName).isEmpty)
    newCerts.foreach(putCert(rw, _))
    putCertsAtHeight(rw, height, newCerts)

    putCrlData(rw, crlHashesByIssuer, crlDataByHash)

    minersBanHistory.foreach { case (address, history) => updateMinerBanHistory(rw, address, history, height) }
    minersCancelledWarnings.foreach((updateMinerCancelledWarnings(rw) _).tupled)

    for ((leaseId, leaseDetails) <- leaseMap) {
      rw.put(LeaseCFKeys.leaseDetails(leaseId), Some(leaseDetails))
    }

    for ((leaseId, leaseDetails) <- leaseCancelMap) {
      rw.put(LeaseCFKeys.leaseDetails(leaseId), Some(leaseDetails))
    }

    val orderedLeasePairs = transactions
      .collect {
        case tx: LeaseTransaction =>
          val leaseId = LeaseId(tx.id())
          Seq(leaseId -> leaseMap(leaseId))

        case tx: ExecutedContractTransactionV3 =>
          val leaseOps = tx.assetOperations.collect {
            case leaseOp: ContractLeaseV1 =>
              val leaseId = LeaseId(leaseOp.leaseId)
              leaseId -> leaseMap(leaseId)
          }
          leaseOps

      }.flatten

    val leasesByAssetHolder = groupOrderedLeasesByAssetHolder(orderedLeasePairs)

    for ((assetHolder, leases) <- leasesByAssetHolder) {
      val leasesForAssetHolderDB = assetHolder match {
        case account @ Account(address) =>
          val addressId = newBalancedAccountsMap.getOrElse(account, addressIdUnsafe(address))
          LeaseCFKeys.leasesForAddress(addressId, storage)
        case contract @ Contract(contractId) =>
          val contractStateId = newBalancedContractsMap.getOrElse(contract, contractStateIdUnsafe(contractId))
          LeaseCFKeys.leasesForContract(contractStateId, storage)
      }

      leasesForAssetHolderDB.addLastN(rw, leases)
    }

    if (mainnetPatch.contains((height, block.uniqueId))) {
      MainnetMigration.apply(rw)
    }
  }

  private def addressIdUnsafe(address: Address): BigInt = {
    addressId(address).getOrElse(throw new RuntimeException(s"Unknown address: $address, can't find id for it"))
  }

  private def contractStateIdUnsafe(contract: ContractId): BigInt = {
    stateIdByContractId(contract).getOrElse(throw new RuntimeException(s"Unknown contract id: $contract, can't find id for it"))
  }

  private def extractAddressUnsafe(addressOrAlias: AddressOrAlias): Address = addressOrAlias match {
    case a: Address => a
    case a: Alias   => resolveAlias(a).getOrElse(throw new RuntimeException(s"Can't resolve alias ${a.stringRepr}"))
  }

  private def appendNonEmptyRoleAddresses(newNonEmptyRoleAddresses: Map[Address, BigInt], rw: MainReadWriteDB): Unit = {
    for ((nonEmptyRoleAddress, id) <- newNonEmptyRoleAddresses) {
      rw.put(WEKeys.nonEmptyRoleAddressId(nonEmptyRoleAddress), Some(id))
      log.trace(s"WRITE non-empty-role address ${nonEmptyRoleAddress.address} -> $id")
      rw.put(WEKeys.idToNonEmptyRoleAddress(id), nonEmptyRoleAddress)
    }

    val lastNonEmptyRoleAddressId = loadMaxNonEmptyRoleAddressId() + newNonEmptyRoleAddresses.size
    rw.put(WEKeys.lastNonEmptyRoleAddressId, Some(lastNonEmptyRoleAddressId))

    log.trace(s"WRITE lastNonEmptyRoleAddressId = $lastNonEmptyRoleAddressId")
  }

  /**
    * Performs state rollback to specified targetBlockId
    * Returns sequence of discarded blocks
    */
  override protected def doRollback(targetBlockId: ByteStr): Seq[Block] = {

    case class AssetInfoInvalidationData(
        assetIdsForInfoInvalidation: Seq[AssetId],
        assetIdsForDiscard: Seq[AssetId],
        leaseInfoForDiscard: Seq[LeaseParticipantsInfo],
        leaseCancelIdsForDiscard: Seq[LeaseId]
    )

    def assetOpsToDataForRollback(contractId: ContractId,
                                  assetOpsRollbackOrder: Seq[com.wavesenterprise.transaction.docker.assets.ContractAssetOperation]) = {
      val assetIdsForInfoInvalidationBuilder = Seq.newBuilder[AssetId]
      val assetIdsForDiscardBuilder          = Seq.newBuilder[AssetId]
      val leaseInfoForDiscardBuilder         = Seq.newBuilder[LeaseParticipantsInfo]
      val leaseCancelIdsForDiscardBuilder    = Seq.newBuilder[LeaseId]

      assetOpsRollbackOrder.foreach {
        case issueOp: ContractIssueV1 =>
          assetIdsForInfoInvalidationBuilder += issueOp.assetId
          assetIdsForDiscardBuilder += issueOp.assetId
        case reissueOp: ContractReissueV1 =>
          assetIdsForInfoInvalidationBuilder += reissueOp.assetId
        case burnOp: ContractBurnV1 if burnOp.assetId.isDefined =>
          assetIdsForInfoInvalidationBuilder += burnOp.assetId.get
        case leaseOp: ContractLeaseV1 =>
          val recipientAddress = extractAddressUnsafe(leaseOp.recipient)
          val leaseInfo        = LeaseParticipantsInfo(LeaseId(leaseOp.leaseId), Contract(contractId), recipientAddress)
          leaseInfoForDiscardBuilder += leaseInfo
        case leaseCancelOp: ContractCancelLeaseV1 =>
          leaseCancelIdsForDiscardBuilder += LeaseId(leaseCancelOp.leaseId)
        case _: ContractTransferOutV1 => ()
      }

      AssetInfoInvalidationData(
        assetIdsForInfoInvalidationBuilder.result(),
        assetIdsForDiscardBuilder.result(),
        leaseInfoForDiscardBuilder.result(),
        leaseCancelIdsForDiscardBuilder.result()
      )
    }

    readOnly(_.get(Keys.heightOf(targetBlockId))).fold(Seq.empty[Block]) { targetHeight =>
      log.debug(s"Rolling back to blockId: '$targetBlockId' at height: '$targetHeight', current height: '$height'")

      val discardedBlocks: Seq[Block] = for (currentHeight <- height until targetHeight by -1) yield {
        val portfoliosToInvalidate = Seq.newBuilder[AssetHolder]
        val assetInfoToInvalidate  = Seq.newBuilder[ByteStr]
        val ordersToInvalidate     = Seq.newBuilder[ByteStr]
        val scriptsToDiscard       = Seq.newBuilder[Address]
        val assetScriptsToDiscard  = Seq.newBuilder[ByteStr]

        val discardedBlock = readWrite { rw =>
          log.trace(s"Rolling back to '${currentHeight - 1}'")
          rw.put(Keys.height, currentHeight - 1)

          val assetIdsToDiscard           = Set.newBuilder[ByteStr]
          val permissionOpsToDiscard      = Seq.newBuilder[(BigInt, PermissionOp)]
          val minersToDiscard             = Seq.newBuilder[(Address, PermissionOp)]
          val validatorsToDiscard         = Seq.newBuilder[(Address, PermissionOp)]
          val participantRegsToDiscard    = Seq.newBuilder[ParticipantRegistration]
          val contractsIdToDiscard        = Set.newBuilder[ByteStr]
          val policyIdsToDiscard          = Set.newBuilder[ByteStr]
          val policyDataHashesToDiscard   = Seq.newBuilder[PolicyDataHashTransaction]
          val aliasesByAddressIdToDiscard = Set.newBuilder[(BigInt, Alias)]
          val policiesDiscardDiffs        = List.newBuilder[Map[ByteStr, PolicyDiff]]
          val contractsKeysToDiscard      = mutable.Map[ContractId, Set[String]]()
          val dataKeysToDiscard           = mutable.Map[(BigInt, Address), Set[String]]()

          val leasesToDiscard         = Seq.newBuilder[LeaseParticipantsInfo]
          val leaseCancelIdsToDiscard = Seq.newBuilder[LeaseId]

          val (discardedHeader, _) = rw
            .get(Keys.blockHeaderAndSizeAt(currentHeight))
            .getOrElse(throw new IllegalArgumentException(s"No block at height $currentHeight"))

          for (addressId <- rw.get(Keys.changedAddresses(currentHeight))) {
            val address = rw.get(Keys.idToAddress(addressId))

            for (assetId <- rw.get(Keys.assetList(addressId))) {
              rw.delete(Keys.assetBalance(addressId, assetId)(currentHeight))
              rw.filterHistory(Keys.assetBalanceHistory(addressId, assetId), currentHeight)
            }

            rw.delete(Keys.westBalance(addressId)(currentHeight))
            rw.filterHistory(Keys.westBalanceHistory(addressId), currentHeight)

            rw.delete(Keys.leaseBalance(addressId)(currentHeight))
            rw.filterHistory(Keys.leaseBalanceHistory(addressId), currentHeight)

            log.trace(s"Discarding portfolio for $address")

            portfoliosToInvalidate += address.toAssetHolder
            addressBalanceAtHeightCache.invalidate((currentHeight, addressId))
            leaseBalanceAtHeightCache.invalidate((currentHeight, addressId))
            discardAddressLeaseBalance(address)

            val kTxSeqNr = Keys.addressTransactionSeqNr(addressId)
            val txSeqNr  = rw.get(kTxSeqNr)
            val kTxIds   = Keys.addressTransactionIds(addressId, txSeqNr)
            for ((_, id) <- rw.get(kTxIds).headOption; (h, _) <- rw.get(Keys.transactionInfo(id)) if h == currentHeight) {
              rw.delete(kTxIds)
              rw.put(kTxSeqNr, (txSeqNr - 1).max(0))
            }
          }

          for (contractStateId <- rw.get(WEKeys.changedContracts(height))) {
            val contractId = rw.get(WEKeys.stateIdToContractId(contractStateId))

            for (assetId <- rw.get(WEKeys.contractAssetList(contractStateId))) {
              rw.delete(WEKeys.contractAssetBalance(contractStateId, assetId)(currentHeight))
              rw.filterHistory(WEKeys.contractAssetBalanceHistory(contractStateId, assetId), currentHeight)
            }

            rw.delete(WEKeys.contractWestBalance(contractStateId)(currentHeight))
            rw.filterHistory(Keys.westBalanceHistory(contractStateId), currentHeight)

            rw.filterHistory(ContractCFKeys.contractLeaseBalanceHistory(contractStateId), currentHeight)

            portfoliosToInvalidate += ContractId(contractId).toAssetHolder
            contractBalanceAtHeightCache.invalidate((currentHeight, contractStateId))
            leaseBalanceAtHeightCache.invalidate((currentHeight, contractStateId))
          }

          val txIdsAtHeight = Keys.transactionIdsAtHeight(currentHeight)
          val oldMinerIds   = rw.get(WEKeys.miners)
          val blockTxIds    = rw.get(Keys.blockTransactionsAtHeight(currentHeight))
          val blockTxs      = Map.newBuilder[ByteStr, Transaction]

          for (txId <- rw.get(txIdsAtHeight).reverse) {
            val ktxId = Keys.transactionInfo(txId)

            for ((_, tx) <- rw.get(ktxId)) {
              tx match {
                case _: GenesisTransaction | _: GenesisPermitTransaction | _: GenesisRegisterNodeTransaction => // genesis transaction can not be rolled back
                case _: TransferTransaction | _: MassTransferTransaction                                     => // balances already restored
                case _: ExecutableTransaction | _: AtomicTransaction                                         => // everything is already rollbacked
                case tx: IssueTransaction =>
                  assetInfoToInvalidate += rollbackAssetInfo(rw, tx.id(), currentHeight)
                  assetIdsToDiscard += tx.id()
                case tx: ReissueTransaction =>
                  assetInfoToInvalidate += rollbackAssetInfo(rw, tx.assetId, currentHeight)
                case tx: BurnTransaction =>
                  assetInfoToInvalidate += rollbackAssetInfo(rw, tx.assetId, currentHeight)
                case tx: SponsorFeeTransaction =>
                  assetInfoToInvalidate += rollbackSponsorship(rw, tx.assetId, currentHeight)
                case tx: LeaseTransaction =>
                  val recipientAddress = tx.recipient match {
                    case a: Address => a
                    case a: Alias   => resolveAlias(a).getOrElse(throw new RuntimeException(s"Can't resolve alias ${a.stringRepr}"))
                  }

                  val leaseDiscardInfo: LeaseParticipantsInfo =
                    LeaseParticipantsInfo(LeaseId(tx.id()), Account(tx.sender.toAddress), recipientAddress)
                  leasesToDiscard += leaseDiscardInfo
                case tx: LeaseCancelTransaction =>
                  leaseCancelIdsToDiscard += LeaseId(tx.leaseId)

                case tx: SetScriptTransaction =>
                  val address = tx.sender.toAddress
                  scriptsToDiscard += address
                  for (addressId <- addressId(address)) {
                    rw.delete(Keys.addressScript(addressId)(currentHeight))
                    rw.filterHistory(Keys.addressScriptHistory(addressId), currentHeight)
                  }

                case tx: SetAssetScriptTransaction =>
                  val asset = tx.assetId
                  assetScriptsToDiscard += asset
                  rw.delete(Keys.assetScript(asset)(currentHeight))
                  rw.filterHistory(Keys.assetScriptHistory(asset), currentHeight)

                case tx: DataTransaction =>
                  val address = tx.sender.toAddress
                  addressId(address).foreach(id => collectKeysToDiscard(address, id, tx, dataKeysToDiscard))

                case tx: CreateAliasTransaction =>
                  rw.delete(Keys.addressIdOfAlias(tx.alias))
                  addressId(tx.sender.toAddress).foreach { id =>
                    aliasesByAddressIdToDiscard += (id -> tx.alias)
                  }

                case tx: ExchangeTransaction =>
                  ordersToInvalidate += rollbackOrderFill(rw, tx.buyOrder.id(), currentHeight)
                  ordersToInvalidate += rollbackOrderFill(rw, tx.sellOrder.id(), currentHeight)

                case tx: PermitTransaction =>
                  resolveAlias(tx.target).foreach { address =>
                    addressId(address).foreach { addressId =>
                      permissionOpsToDiscard += (addressId -> tx.permissionOp)
                      tx.permissionOp.role match {
                        case Role.Miner             => minersToDiscard += (address     -> tx.permissionOp)
                        case Role.ContractValidator => validatorsToDiscard += (address -> tx.permissionOp)
                        case _                      => ()
                      }
                    }
                  }

                case tx: RegisterNodeTransaction =>
                  participantRegsToDiscard += ParticipantRegistration(tx.target.toAddress, tx.target, tx.opType)

                case tx: ExecutedContractTransaction =>
                  rw.delete(WEKeys.executedTxIdFor(tx.tx.id()))
                  val contractId = ContractId(tx.tx.contractId)
                  tx.tx match {
                    case _: CreateContractTransaction =>
                      contractsIdToDiscard += contractId.byteStr
                      rollbackContractInfo(rw, contractId, currentHeight)
                      collectKeysToDiscard(tx, contractsKeysToDiscard)

                    case _: CallContractTransaction =>
                      collectKeysToDiscard(tx, contractsKeysToDiscard)

                    case _: UpdateContractTransaction =>
                      rollbackContractInfo(rw, contractId, currentHeight)
                  }

                  tx match {
                    case executedTxV3: ExecutedContractTransactionV3 =>
                      val contractId = ContractId(executedTxV3.tx.contractId)
                      val AssetInfoInvalidationData(assetIdsForInfoInvalidation, assetIdsForDiscard, leaseInfoForDiscard, leaseCancelIdsForDiscard) =
                        assetOpsToDataForRollback(contractId, executedTxV3.assetOperations.reverse)

                      assetInfoToInvalidate ++= assetIdsForInfoInvalidation.map(rollbackAssetInfo(rw, _, currentHeight))
                      assetIdsToDiscard ++= assetIdsForDiscard
                      collectKeysToDiscard(tx, contractsKeysToDiscard)

                      leasesToDiscard ++= leaseInfoForDiscard
                      leaseCancelIdsToDiscard ++= leaseCancelIdsForDiscard
                    case _ => ()
                  }

                case tx: DisableContractTransaction =>
                  rollbackContractInfo(rw, ContractId(tx.contractId), currentHeight)

                case tx: CreatePolicyTransaction =>
                  policiesDiscardDiffs += Map(tx.id() -> PolicyDiffValue.fromTx(tx).negate())
                  policyIdsToDiscard += tx.id()

                case tx: UpdatePolicyTransaction =>
                  policiesDiscardDiffs += Map(tx.policyId -> PolicyDiffValue.fromTx(tx).negate())

                case tx: PolicyDataHashTransaction =>
                  policyDataHashesToDiscard += tx

                case unsupportedTx =>
                  throw new RuntimeException(s"Fatal error: rollback case not implemented for transaction '$unsupportedTx'")
              }
              if (blockTxIds.contains(txId)) {
                blockTxs += (txId -> tx)
              }
            }

            rw.delete(ktxId)
          }

          val previousBlockTimestamp = rw
            .get(Keys.blockHeaderAndSizeAt(currentHeight - 1))
            .map(_._1)
            .getOrElse(throw new IllegalArgumentException(s"No block at height '${currentHeight - 1}'"))
            .timestamp

          val resultPermissionOpsToDiscard = permissionOpsToDiscard.result()
          rollbackParticipants(rw, participantRegsToDiscard.result())
          rollbackPermissions(rw, resultPermissionOpsToDiscard)
          rollbackMiners(rw, minersToDiscard.result())
          rollbackValidators(rw, validatorsToDiscard.result())
          rollbackContractsId(rw, contractsIdToDiscard.result())
          rollbackMinerBanHistory(rw, oldMinerIds, previousBlockTimestamp, currentHeight - 1)
          rollbackPolicyDataHashes(rw, policyDataHashesToDiscard.result())
          rollbackCreatedPolicies(rw, policyIdsToDiscard.result())
          rollbackIssuedAssets(rw, assetIdsToDiscard.result())
          rollbackIssuedAliases(aliasesByAddressIdToDiscard.result())
          dataKeysToDiscard.foreach {
            case (Tuple2(addressId, address), keys) => rollbackAccountData(rw, address, addressId, keys, currentHeight)
          }
          contractsKeysToDiscard.foreach {
            case (id, keys) => rollbackContractData(rw, id.byteStr, keys, currentHeight)
          }

          for ((policyId, policyDiff: PolicyDiffValue) <- policiesDiscardDiffs.result.combineAll) {
            updatePolicy(rw, policyId, policyDiff)
          }

          rollbackLeaseActions(rw, leasesToDiscard.result(), leaseCancelIdsToDiscard.result())

          rw.delete(txIdsAtHeight)
          rw.delete(Keys.blockHeaderAndSizeAt(currentHeight))
          rw.delete(Keys.blockTransactionsAtHeight(currentHeight))
          rw.delete(Keys.heightOf(discardedHeader.signerData.signature))
          rw.delete(Keys.carryFee(currentHeight))
          rw.filterHistory(Keys.carryFeeHistory, currentHeight)

          invalidatePermissionsCache(resultPermissionOpsToDiscard.map { case (addressId, _) => addressId })

          rollbackBlockchainFeature(rw, targetHeight)

          val blockTxsResult = blockTxs.result()
          Block.build(discardedHeader, blockTxIds.map(blockTxsResult)).explicitGet()
        }

        portfoliosToInvalidate.result().foreach(_.product(discardAddressPortfolio, discardContractPortfolio))
        assetInfoToInvalidate.result().foreach(discardAssetDescription)
        ordersToInvalidate.result().foreach(discardVolumeAndFee)
        scriptsToDiscard.result().foreach(discardScript)
        assetScriptsToDiscard.result().foreach(discardAssetScript)
        invalidateMinerBanHistoryCache()
        discardedBlock
      }

      log.debug(s"Rollback to block $targetBlockId at $targetHeight completed")

      discardedBlocks.reverse
    }
  }

  private def rollbackBlockchainFeature(rw: MainReadWriteDB, targetHeight: Int): Unit = {
    approvedFeaturesCache = approvedFeaturesCache.filter { case (_, featureHeight) => featureHeight <= targetHeight }
    rw.put(Keys.approvedFeatures, approvedFeaturesCache)

    activatedFeaturesCache = approvedFeaturesCache.mapValues(_ + fs.featureCheckBlocksPeriod) ++ fs.preActivatedFeatures
    rw.put(Keys.activatedFeatures, activatedFeaturesCache)
  }

  private def collectKeysToDiscard(address: Address,
                                   addressId: BigInt,
                                   tx: DataTransaction,
                                   keysToDiscard: mutable.Map[(BigInt, Address), Set[String]]): Unit = {
    val key     = addressId -> address
    val updated = keysToDiscard.getOrElse(key, Set.empty) ++ tx.data.map(_.key)
    keysToDiscard += key -> updated
  }

  private def rollbackAccountData(rw: MainReadWriteDB, address: Address, addressId: BigInt, keys: Set[String], currentHeight: Int): Unit = {
    val dataKeys = Keys.dataKeys(addressId, storage)
    val orphanedKeys = for {
      key <- keys
      historyKey = Keys.dataHistory(addressId, key)
      filtered = {
        log.trace(s"Discarding '$key' for '$address' at '$currentHeight'")
        rw.delete(Keys.data(addressId, key)(currentHeight))
        rw.filterHistory(historyKey, currentHeight)
      }
      if filtered.isEmpty
    } yield {
      rw.delete(historyKey)
      key
    }
    if (orphanedKeys.nonEmpty) {
      dataKeys.remove(rw, orphanedKeys)
    }
  }

  private def invalidatePermissionsCache(addressIds: Iterable[BigInt]): Unit =
    addressIds.foreach(permissionsCache.invalidate)

  private def rollbackCreatedPolicies(rw: MainReadWriteDB, policies: Set[ByteStr]): Unit = {
    policyIdsSet.remove(rw, policies)
  }

  private def rollbackPolicyDataHashes(rw: MainReadWriteDB, policyIdsWithDataHashes: Seq[PolicyDataHashTransaction]): Unit = {
    policyIdsWithDataHashes
      .groupBy(tx => tx.policyId)
      .foreach {
        case (policyId, txsGroup) =>
          updatePolicyDataHashes(rw, policyId, txsGroup.toSet, OpType.Remove)
      }
  }

  private def rollbackAssetInfo(rw: MainReadWriteDB, assetId: ByteStr, currentHeight: Int): ByteStr = {
    rw.delete(Keys.assetInfo(assetId)(currentHeight))
    rw.filterHistory(Keys.assetInfoHistory(assetId), currentHeight)
    assetId
  }

  private def rollbackIssuedAssets(rw: MainReadWriteDB, assets: Set[ByteStr]): Unit = {
    assetIdsSet.remove(rw, assets)
  }

  private def rollbackOrderFill(rw: MainReadWriteDB, orderId: ByteStr, currentHeight: Int): ByteStr = {
    rw.delete(Keys.filledVolumeAndFee(orderId)(currentHeight))
    rw.filterHistory(Keys.filledVolumeAndFeeHistory(orderId), currentHeight)
    orderId
  }

  private def rollbackSponsorship(rw: MainReadWriteDB, assetId: ByteStr, currentHeight: Int): ByteStr = {
    rw.delete(Keys.sponsorship(assetId)(currentHeight))
    rw.filterHistory(Keys.sponsorshipHistory(assetId), currentHeight)
    assetId
  }

  private def rollbackIssuedAliases(issuedAliasesByAddressId: Set[(BigInt, Alias)]): Unit = {
    issuedAliasesByAddressId
      .groupBy { case (addressId, _) => addressId }
      .mapValues(_.map { case (_, alias) => alias })
      .foreach {
        case (addressId, aliases) =>
          Keys.issuedAliasesByAddressId(addressId, storage).remove(aliases)
      }
  }

  /**
    * Discarding permissions from PermitTransactions of current block
    */
  private def rollbackPermissions(rw: MainReadWriteDB, addressIdToPermissionOpSeq: Seq[(BigInt, PermissionOp)]): Unit = {
    addressIdToPermissionOpSeq
      .groupBy({ case (addressId, _) => addressId })
      .mapValues(_.map(_._2))
      .foreach {
        case (addressId, permissionOpsToDelete) =>
          val oldPermissionOps = rw.get(WEKeys.permissions(addressId))

          val filteredPermOps = oldPermissionOps
            .map(_.diff(permissionOpsToDelete))
            .filter(_.nonEmpty)

          if (oldPermissionOps.isDefined && filteredPermOps.isEmpty) {
            rw.delete(WEKeys.permissions(addressId))
          } else {
            rw.put(WEKeys.permissions(addressId), filteredPermOps)
          }
      }
  }

  /**
    * Discarding records from miners queue
    */
  private def rollbackMiners(rw: MainReadWriteDB, minerAddressToPermissionOpSeq: Seq[(Address, PermissionOp)]): Unit = {
    val oldMinerQueue = miners
    val updateMinerQueue = minerAddressToPermissionOpSeq.foldLeft(oldMinerQueue) {
      case (mq, (minerAddress, permissionOpToDelete)) =>
        mq.remove(minerAddress, permissionOpToDelete)
    }
    val updatedMiners = updateMinerQueue.suggestedMiners
      .flatMap(addr => addressId(addr).toSeq)
    rw.put(WEKeys.miners, updatedMiners)
  }

  private def rollbackValidators(rw: MainReadWriteDB, validatorAddressToPermissionOpSeq: Seq[(Address, PermissionOp)]): Unit = {
    val updatedValidatorPool = validatorAddressToPermissionOpSeq.foldLeft(contractValidators) {
      case (pool, (address, permission)) => pool.remove(address, permission)
    }

    val updatedValidatorIds = updatedValidatorPool.suggestedValidators.toSeq.flatMap(addressId)

    rw.put(WEKeys.validators, updatedValidatorIds)
  }

  /**
    * Discarding records from MinerBanHistory
    */
  private def rollbackMinerBanHistory(rw: MainReadWriteDB, oldMinerIds: Seq[BigInt], targetBlockTimestamp: Long, targetHeight: Int): Unit = {
    log.trace(s"rollbackMinerBanHistory, oldMinerIds: [${oldMinerIds.mkString(", ")}]")

    oldMinerIds.foreach { minerId =>
      val oldBanHistoryEntries                                  = minerBanHistoryEntries(minerId, targetHeight)
      val (maybeKnownOldHistorySize, oldHistoryNewEntriesCount) = minerBanHistorySizeAndNewEntriesCount(minerId, oldBanHistoryEntries)

      val cancelledWarnings = popCancelledWarnings(rw)(minerId, targetBlockTimestamp)
      maybeBanHistoryBuilder(targetHeight).map { historyBuilder =>
        val targetBanHistoryOrdering = MinerBanHistory.selectOrdering(activatedFeatures, targetHeight)

        val updatedHistory = historyBuilder
          .build(oldBanHistoryEntries, maybeKnownOldHistorySize, oldHistoryNewEntriesCount)
          .rollback(targetBlockTimestamp, cancelledWarnings, targetBanHistoryOrdering)
        updateMinerBanHistory(rw, minerId, updatedHistory, targetHeight)
      }
    }
  }

  private def minerBanHistorySizeAndNewEntriesCount(addressId: BigInt, entries: Seq[MinerBanlistEntry]): (Option[Int], Int) = {
    val lastV2EntryId   = lastMinerBanHistoryV2EntryId(addressId)
    val v2AlreadyFilled = lastV2EntryId > 0

    if (v2AlreadyFilled)
      Some(lastV2EntryId.toInt) -> 0
    else
      None -> entries.size
  }

  private def popCancelledWarnings(rw: MainReadWriteDB)(minerId: BigInt, targetTimestamp: Long): Seq[CancelledWarning] = {
    val key                  = WEKeys.minerCancelledWarnings(minerId)
    val allCancelledWarnings = rw.get(key).getOrElse(Seq.empty)
    val (filtered, rest)     = allCancelledWarnings.partition(_.cancellationTimestamp > targetTimestamp)
    val trimmedRest          = rest.take(maxRollbackDepth)
    rw.put(key, Some(trimmedRest))
    filtered
  }

  /**
    * Discarding participant registrations from RegisterNodeTransaction
    */
  private def rollbackParticipants(rw: MainReadWriteDB, dirtyRegs: Seq[ParticipantRegistration]): Unit = {
    import com.wavesenterprise.state.Diff.{participantsReqSemigroup, recombine}

    val participantRegsCleaned = recombine(dirtyRegs)

    val (idsToDiscard, idsToRecover) = participantRegsCleaned.foldLeft((Seq.empty[BigInt], Seq.empty[BigInt])) {
      case ((toDiscard, toRecover), reg) =>
        addressId(reg.address)
          .map { addressId =>
            reg.opType match {
              case OpType.Add =>
                rw.delete(WEKeys.participantPubKey(addressId))
                (addressId +: toDiscard, toRecover)
              case OpType.Remove =>
                rw.put(WEKeys.participantPubKey(addressId), Some(reg.pubKey))
                (toDiscard, addressId +: toRecover)
            }
          }
          .getOrElse(toDiscard -> toRecover)
    }

    val currentParticipants = rw.get(WEKeys.networkParticipants())
    val updatedParticipants = currentParticipants.diff(idsToDiscard).union(idsToRecover)
    rw.put(WEKeys.networkParticipants(), updatedParticipants)
  }

  private def rollbackContractsId(rw: MainReadWriteDB, contractsIdToDiscard: Set[ByteStr]): Unit = {
    contractIdsSet.remove(rw, contractsIdToDiscard)
  }

  private def rollbackContractInfo(rw: MainReadWriteDB, contractId: ContractId, currentHeight: Int): Unit = {
    rw.delete(WEKeys.contract(contractId.byteStr)(currentHeight))
    rw.filterHistory(WEKeys.contractHistory(contractId.byteStr), currentHeight)
  }

  private def rollbackContractData(rw: MainReadWriteDB, contractId: ByteStr, keys: Set[String], currentHeight: Int): Unit = {
    val contractKeys = WEKeys.contractKeys(contractId, storage)
    val orphanedKeys = for {
      key <- keys
      historyKey = WEKeys.contractDataHistory(contractId, key)
      filtered = {
        log.trace(s"Discarding '$key' for '$contractId' contract at '$currentHeight'")
        rw.delete(WEKeys.contractData(contractId, key)(currentHeight))
        rw.filterHistory(historyKey, currentHeight)
      }
      if filtered.isEmpty
    } yield {
      rw.delete(historyKey)
      key
    }
    if (orphanedKeys.nonEmpty) {
      contractKeys.remove(rw, orphanedKeys)
    }
  }

  private def groupOrderedLeasesByAssetHolder(leasePairs: Seq[(LeaseId, LeaseDetails)]): Map[AssetHolder, Seq[LeaseId]] = {
    val leasesByAssetHolder = scala.collection.mutable.Map[AssetHolder, Seq[LeaseId]]().withDefaultValue(Seq())

    leasePairs.foreach { case (leaseId, leaseDetails) =>
      val sender = leaseDetails.sender
      val recipient = {
        val recipientAddress = leaseDetails.recipient match {
          case a: Address => a
          case a: Alias   => resolveAlias(a).getOrElse(throw new RuntimeException(s"Can't resolve alias ${a.stringRepr}"))
        }
        Account(recipientAddress)
      }

      leasesByAssetHolder(sender) = leasesByAssetHolder(sender) :+ leaseId
      leasesByAssetHolder(recipient) = leasesByAssetHolder(recipient) :+ leaseId
    }

    leasesByAssetHolder.toMap
  }

  private def rollbackLeaseActions(rw: MainReadWriteDB, leasesRollbackOrdered: Seq[LeaseParticipantsInfo], leaseCancelIds: Seq[LeaseId]) = {
    val leasesByAssetHolder = scala.collection.mutable.Map[AssetHolder, Seq[LeaseId]]().withDefaultValue(Seq())

    leasesRollbackOrdered.foreach { lease =>
      rw.delete(LeaseCFKeys.leaseDetails(lease.leaseId))
      leasesByAssetHolder(lease.sender) = lease.leaseId +: leasesByAssetHolder(lease.sender)
      leasesByAssetHolder(Account(lease.recipient)) = lease.leaseId +: leasesByAssetHolder(Account(lease.recipient))
    }

    leasesByAssetHolder.foreach { case (assetHolder, leaseIds) =>
      val storedLeaseIds = assetHolder match {
        case Account(address) =>
          val addressId = loadAddressId(address).get
          LeaseCFKeys.leasesForAddress(addressId, storage)
        case Contract(contractId) =>
          val contractStateId = stateIdByContractId(contractId).get
          LeaseCFKeys.leasesForContract(contractStateId, storage)
      }

      val lastStored = storedLeaseIds.takeRight(leaseIds.size)

      if (lastStored != leaseIds) {
        throw new RuntimeException(s"Wrong unexpected leases' ids order for ${assetHolder.description}")

      }

      storedLeaseIds.pollLastN(leaseIds.size)
    }

    leaseCancelIds.map(leaseId =>
      rw.update(LeaseCFKeys.leaseDetails(leaseId))(leaseDetails => leaseDetails.map(_.copy(isActive = true))))

  }

  override def transactionInfo(id: ByteStr): Option[(Int, Transaction)] = readOnly(transactionInfo(id, _))

  protected def transactionInfo(id: ByteStr, db: MainReadOnlyDB): Option[(Int, Transaction)] = {
    db.get(Keys.transactionInfo(id))
  }

  override def transactionHeight(id: ByteStr): Option[Int] = readOnly(transactionInfo(id, _)).map(_._1)

  override def containsTransaction(id: ByteStr): Boolean = readOnly(_.has(Keys.transactionInfo(id)))

  override def addressTransactions(address: Address,
                                   txTypes: Set[Transaction.Type],
                                   count: Int,
                                   fromId: Option[AssetId]): Either[String, Seq[(Int, Transaction)]] = readOnly { db =>
    AddressTransactions(db, address, txTypes, count, fromId)
  }

  override def resolveAlias(alias: Alias): Either[ValidationError, Address] = readOnly { db =>
    db.get(Keys.addressIdOfAlias(alias))
      .map(addressId => db.get(Keys.idToAddress(addressId)))
      .toRight(AliasDoesNotExist(alias))
  }

  override def leaseDetails(leaseId: LeaseId): Option[LeaseDetails] = readOnly { db =>
    db.get(LeaseCFKeys.leaseDetails(leaseId))
  }

  // These two caches are used exclusively for balance snapshots. They are not used for portfolios, because there aren't
  // as many miners, so snapshots will rarely be evicted due to overflows.

  import java.lang.{Long => JLong}

  private val addressBalanceAtHeightCache = CacheBuilder
    .newBuilder()
    .maximumSize(100000)
    .recordStats()
    .build[(Int, BigInt), JLong]()

  private val contractBalanceAtHeightCache = CacheBuilder
    .newBuilder()
    .maximumSize(10000)
    .recordStats()
    .build[(Int, BigInt), JLong]

  private val leaseBalanceAtHeightCache = CacheBuilder
    .newBuilder()
    .maximumSize(100000)
    .recordStats()
    .build[(Int, BigInt), LeaseBalance]()

  private val contractLeaseBalanceAtHeightCache = CacheBuilder
    .newBuilder()
    .maximumSize(10000)
    .recordStats()
    .build[(Int, BigInt), LeaseBalance]()

  override def addressBalanceSnapshots(address: Address, from: Int, to: Int): Seq[BalanceSnapshot] = readOnly { db =>
    db.get(Keys.addressId(address)).fold(Seq(BalanceSnapshot(1, 0, 0, 0))) { addressId =>
      val wbh = slice(db.get(Keys.westBalanceHistory(addressId)), from, to)
      val lbh = slice(db.get(Keys.leaseBalanceHistory(addressId)), from, to)
      for {
        (wh, lh) <- merge(wbh, lbh)
        wb = addressBalanceAtHeightCache.get((wh, addressId), () => db.get(Keys.westBalance(addressId)(wh)))
        lb = leaseBalanceAtHeightCache.get((lh, addressId), () => db.get(Keys.leaseBalance(addressId)(lh)))
      } yield BalanceSnapshot(wh.max(lh), wb, lb.in, lb.out)
    }
  }

  override def contractBalanceSnapshots(contractId: ContractId, from: Int, to: Int): Seq[BalanceSnapshot] = readOnly { db =>
    db.get(WEKeys.contractIdToStateId(contractId.byteStr)).fold(Seq(BalanceSnapshot(1, 0, 0, 0))) { contractStateId =>
      val wbh = slice(db.get(WEKeys.contractWestBalanceHistory(contractStateId)), from, to)
      val lbh = slice(db.get(ContractCFKeys.contractLeaseBalanceHistory(contractStateId)), from, to)
      for {
        (wh, lh) <- merge(wbh, lbh)
        wb = contractBalanceAtHeightCache.get((wh, contractStateId), () => db.get(WEKeys.contractWestBalance(contractStateId)(wh)))
        lb = contractLeaseBalanceAtHeightCache.get((lh, contractStateId), () => db.get(ContractCFKeys.contractLeaseBalance(contractStateId)(lh)))
      } yield BalanceSnapshot(wh.max(lh), wb, lb.in, lb.out)
    }
  }

  /**
    * @todo move this method to `object RocksDBWriter` once SmartAccountTrading is activated
    */
  private[database] def merge(wbh: Seq[Int], lbh: Seq[Int]): Seq[(Int, Int)] = {

    /**
      * Compatibility implementation where
      *  {{{([15, 12, 3], [12, 5]) => [(15, 12), (12, 12), (3, 12), (3, 5)]}}}
      *
      * @todo remove this method once SmartAccountTrading is activated
      */
    @tailrec
    def recMergeCompat(wh: Int, wt: Seq[Int], lh: Int, lt: Seq[Int], buf: ArrayBuffer[(Int, Int)]): ArrayBuffer[(Int, Int)] = {
      buf += wh -> lh
      if (wt.isEmpty && lt.isEmpty) {
        buf
      } else if (wt.isEmpty) {
        recMergeCompat(wh, wt, lt.head, lt.tail, buf)
      } else if (lt.isEmpty) {
        recMergeCompat(wt.head, wt.tail, lh, lt, buf)
      } else {
        if (wh >= lh) {
          recMergeCompat(wt.head, wt.tail, lh, lt, buf)
        } else {
          recMergeCompat(wh, wt, lt.head, lt.tail, buf)
        }
      }
    }

    /**
      * Fixed implementation where
      *  {{{([15, 12, 3], [12, 5]) => [(15, 12), (12, 12), (3, 5)]}}}
      */
    @tailrec
    def recMergeFixed(wh: Int, wt: Seq[Int], lh: Int, lt: Seq[Int], buf: ArrayBuffer[(Int, Int)]): ArrayBuffer[(Int, Int)] = {
      buf += wh -> lh
      if (wt.isEmpty && lt.isEmpty) {
        buf
      } else if (wt.isEmpty) {
        recMergeFixed(wh, wt, lt.head, lt.tail, buf)
      } else if (lt.isEmpty) {
        recMergeFixed(wt.head, wt.tail, lh, lt, buf)
      } else {
        if (wh == lh) {
          recMergeFixed(wt.head, wt.tail, lt.head, lt.tail, buf)
        } else if (wh > lh) {
          recMergeFixed(wt.head, wt.tail, lh, lt, buf)
        } else {
          recMergeFixed(wh, wt, lt.head, lt.tail, buf)
        }
      }
    }

    val recMerge = activatedFeatures
      .get(BlockchainFeature.SmartAccountTrading.id)
      .filter(_ <= height)
      .fold(recMergeCompat _)(_ => recMergeFixed)

    recMerge(wbh.head, wbh.tail, lbh.head, lbh.tail, ArrayBuffer.empty)
  }

  override def collectAddressLposPortfolios[A](pf: PartialFunction[(Address, Portfolio), A]): Map[Address, A] = readOnly { db =>
    val b = Map.newBuilder[Address, A]
    for (id <- BigInt(1) to db.get(Keys.lastAddressId).getOrElse(BigInt(0))) {
      val address = db.get(Keys.idToAddress(id))
      pf.runWith(b += address -> _)(address -> loadLposAddressPortfolio(db, id))
    }
    b.result()
  }

  def loadScoreOf(blockId: ByteStr): Option[BigInt] = {
    readOnly(db => db.get(Keys.heightOf(blockId)).map(h => db.get(Keys.score(h))))
  }

  override def loadBlockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = {
    storage.get(Keys.blockHeaderAndSizeAt(height))
  }

  def loadBlockHeaderAndSize(height: Int, db: MainReadOnlyDB): Option[(BlockHeader, Int)] = {
    db.get(Keys.blockHeaderAndSizeAt(height))
  }

  override def loadBlockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)] = {
    storage.get(Keys.heightOf(blockId)).flatMap(loadBlockHeaderAndSize)
  }

  def loadBlockHeaderAndSize(blockId: ByteStr, db: MainReadOnlyDB): Option[(BlockHeader, Int)] = {
    db.get(Keys.heightOf(blockId))
      .flatMap(loadBlockHeaderAndSize(_, db))
  }

  override def loadBlockBytes(h: Int): Option[Array[Byte]] = readOnly { db =>
    val headerKey = Keys.blockHeaderBytesAt(h)
    db.get(headerKey).map { headerBytes =>
      val blockHeader = BlockHeader.parse(headerBytes)
      val txBytes     = readBlockTransactionBytes(h, db)
      val out         = newDataOutput(headerBytes.length + txBytes.length)
      out.writeAsBlockBytes(blockHeader, txBytes)
      out.toByteArray
    }
  }

  override def loadBlockBytes(blockId: ByteStr): Option[Array[Byte]] = {
    readOnly(db => db.get(Keys.heightOf(blockId))).flatMap(h => loadBlockBytes(h))
  }

  override def loadHeightOf(blockId: ByteStr): Option[Int] = {
    readOnly(_.get(Keys.heightOf(blockId)))
  }

  override def lastBlockIds(howMany: Int): immutable.IndexedSeq[ByteStr] = readOnly { db =>
    // since this is called from outside of the main blockchain updater thread, instead of using cached height,
    // explicitly read height from storage to make this operation atomic.
    val currentHeight = db.get(Keys.height)
    (currentHeight until (currentHeight - howMany).max(0) by -1)
      .map(h => db.get(Keys.blockHeaderAndSizeAt(h)).get._1.signerData.signature)
  }

  override def lastBlockIds(startBlock: BlockId, howMany: Int): Option[Seq[BlockId]] = readOnly { db =>
    db.get(Keys.heightOf(startBlock)).map { startHeight =>
      (startHeight until (startHeight - howMany).max(0) by -1).map { height =>
        db.get(Keys.blockHeaderAndSizeAt(height)) match {
          case Some((header, _)) => header.signerData.signature
          case None              => throw new RuntimeException(s"Block at height '$height' not found")
        }
      }
    }
  }

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int): Option[Seq[ByteStr]] = readOnly { db =>
    db.get(Keys.heightOf(parentSignature)).map { parentHeight =>
      (parentHeight until (parentHeight + howMany))
        .flatMap { h =>
          db.get(Keys.blockHeaderAndSizeAt(h))
        }
        .map { b =>
          b._1.signerData.signature
        }
    }
  }

  override def parent(block: Block, back: Int): Option[Block] = readOnly { db =>
    db.get(Keys.heightOf(block.reference)).flatMap(h => loadBlock(h - back + 1))
  }

  override def parentHeader(block: Block): Option[BlockHeader] = readOnly { db =>
    loadBlockHeaderAndSize(block.reference, db).map { case (header, _) => header }
  }

  override def featureVotes(height: Int): Map[Short, Int] = readOnly { db =>
    fs.activationWindow(height)
      .flatMap { h =>
        loadBlockHeaderAndSize(h, db).fold(Set.empty[Short]) {
          case (header, _) => header.featureVotes
        }
      }
      .groupBy(identity)
      .mapValues(_.size)
  }

  override def addressAssetDistribution(assetId: ByteStr): AssetDistribution = readOnly { db =>
    val dst = (for {
      seqNr     <- (1 to db.get(Keys.addressesForAssetSeqNr(assetId))).par
      addressId <- db.get(Keys.addressesForAsset(assetId, seqNr)).par
      actualHeight <- db
        .get(Keys.assetBalanceHistory(addressId, assetId))
        .filterNot(_ > height)
        .headOption
      balance = db.get(Keys.assetBalance(addressId, assetId)(actualHeight))
      if balance > 0
    } yield db.get(Keys.idToAddress(addressId)) -> balance).toMap.seq

    AssetDistribution(dst)
  }

  override def addressAssetDistributionAtHeight(assetId: AssetId,
                                                height: Int,
                                                count: Int,
                                                fromAddress: Option[Address]): Either[ValidationError, AssetDistributionPage] = readOnly { db =>
    val canGetAfterHeight = db.get(Keys.safeRollbackHeight)

    lazy val maybeAddressId = fromAddress.flatMap(addr => db.get(Keys.addressId(addr)))

    def takeAfter(s: Seq[BigInt], a: Option[BigInt]): Seq[BigInt] = {
      a match {
        case None    => s
        case Some(v) => s.dropWhile(_ != v).drop(1)
      }
    }

    val addressIds: Seq[BigInt] = {
      val all = for {
        seqNr <- 1 to db.get(Keys.addressesForAssetSeqNr(assetId))
        addressId <- db
          .get(Keys.addressesForAsset(assetId, seqNr))
      } yield addressId

      takeAfter(all, maybeAddressId)
    }

    val distribution: Stream[(Address, Long)] =
      for {
        addressId <- addressIds.toStream
        history = db.get(Keys.assetBalanceHistory(addressId, assetId))
        actualHeight <- history.filterNot(_ > height).headOption
        balance = db.get(Keys.assetBalance(addressId, assetId)(actualHeight))
        if balance > 0
      } yield db.get(Keys.idToAddress(addressId)) -> balance

    lazy val page: AssetDistributionPage = {
      val items   = distribution.take(count)
      val hasNext = addressIds.length > count
      val lastKey = items.lastOption.map(_._1)

      val result: Paged[Address, AssetDistribution] =
        Paged(hasNext, lastKey, AssetDistribution(items.toMap))

      AssetDistributionPage(result)
    }

    Either
      .cond(
        height > canGetAfterHeight,
        page,
        GenericError(s"Cannot get asset distribution at height less than ${canGetAfterHeight + 1}")
      )
  }

  override def permissions(acc: Address): Permissions = readOnly { db =>
    permissions(acc, db)
  }

  private def permissions(acc: Address, db: MainReadOnlyDB): Permissions =
    addressId(acc).fold(Permissions.empty)(permissions)

  protected def loadPermissions(addressId: BigInt): Permissions =
    storage.get(WEKeys.permissions(addressId)).fold(Permissions.empty)(Permissions.apply)

  override def miners: MinerQueue = {
    val minersIds = storage.get(WEKeys.miners)
    val permissionsMap =
      (for {
        addressId <- minersIds
        address = idToAddress(addressId)
        perms   = permissions(addressId)
      } yield address -> perms).toMap

    MinerQueue(permissionsMap)
  }

  override def contractValidators: ContractValidatorPool = {
    val validatorIds = storage.get(WEKeys.validators)
    val permissionsMap =
      (for {
        addressId <- validatorIds
        address = idToAddress(addressId)
        perms   = permissions(addressId)
      } yield address -> perms).toMap

    ContractValidatorPool(permissionsMap)
  }

  protected def loadLastBlockContractValidators: Set[Address] =
    lastBlock.fold {
      throw new RuntimeException("Last block not found. It is impossible to get a set of current contract validators.")
    } { block =>
      contractValidators.currentValidatorSet(block.timestamp)
    }

  override def loadMinerBanHistory(address: Address): MinerBanHistory = {
    val banlistBuilder = maybeBanHistoryBuilder(height)
      .getOrElse(throw new IllegalStateException(s"RocksDBWriter.minerBanHistory() called, but node is not running on PoA config"))

    addressId(address)
      .fold(banlistBuilder.empty) { addressId =>
        val entries                           = minerBanHistoryEntries(addressId, height)
        val (maybeKnownSize, newEntriesCount) = minerBanHistorySizeAndNewEntriesCount(addressId, entries)

        banlistBuilder.build(entries, maybeKnownSize, newEntriesCount)
      }
  }

  private def minerBanHistoryEntries(addressId: BigInt, targetHeight: Int): Seq[MinerBanlistEntry] = {
    if (activatedFeatures.get(BlockchainFeature.MinerBanHistoryOptimisationFix.id).exists(_ <= targetHeight))
      minerBanHistoryV2Entries(addressId)
    else
      minerBanHistoryV1Entries(addressId)
  }

  override protected def loadLastMinerBanHistoryV2EntryId(addressId: BigInt): BigInt =
    readOnly(_.get(WEKeys.lastMinerBanHistoryV2EntryId(addressId))).getOrElse(BigInt(0))

  private def minerBanHistoryV2Entries(addressId: BigInt): Seq[MinerBanlistEntry] = {
    def recursiveLoad(entryId: BigInt): Stream[MinerBanlistEntry] = {
      if (entryId > 0) {
        val entry = readOnly(_.get(WEKeys.idToMinerBanHistoryV2Entry(entryId, addressId)))
        log.trace(s"Miner-ban-history-v2 entry for address-id '$addressId' and entry-id '$entryId': $entry")
        entry #:: recursiveLoad(entryId - 1)
      } else {
        Stream.empty
      }
    }

    val lastV2EntryId = lastMinerBanHistoryV2EntryId(addressId)
    log.trace(s"Last miner-ban-history-v2 entry-id for address-id '$addressId': '$lastV2EntryId'")
    val v2AlreadyFilled = lastV2EntryId > 0

    if (v2AlreadyFilled) {
      recursiveLoad(lastV2EntryId)
    } else {
      minerBanHistoryV1Entries(addressId)
    }
  }

  private def minerBanHistoryV1Entries(addressId: BigInt): Seq[MinerBanlistEntry] =
    readOnly(_.get(WEKeys.minerBanHistoryV1(addressId))).getOrElse(Seq.empty)

  override def bannedMiners(height: Int): Seq[Address] = readOnly { db =>
    for {
      addressId <- db.get(WEKeys.miners)
      address    = db.get(Keys.idToAddress(addressId))
      banHistory = minerBanHistory(address)
      if banHistory.isBanned(height)
    } yield address
  }

  override def warningsAndBans: Map[Address, Seq[MinerBanlistEntry]] = readOnly { db =>
    (for {
      addressId <- db.get(WEKeys.miners)
      address     = db.get(Keys.idToAddress(addressId))
      banHistory  = minerBanHistory(address)
      warnAndBans = banHistory.entries
    } yield address -> warnAndBans).toMap
  }

  override def participantPubKey(address: Address): Option[PublicKeyAccount] = readOnly { db =>
    addressId(address).flatMap(addressId => db.get(WEKeys.participantPubKey(addressId)))
  }

  override def networkParticipants(): Seq[Address] = readOnly { db =>
    for {
      addressId <- db.get(WEKeys.networkParticipants())
      address = db.get(Keys.idToAddress(addressId))
    } yield address
  }

  override protected def contractsIdSet(): Set[ByteStr] =
    contractIdsSet.members

  override def contracts(): Set[ContractInfo] =
    contractsIdSet().map(contractIdByteStr => contract(ContractId(contractIdByteStr))).map(_.get)

  override def contract(contractId: ContractId): Option[ContractInfo] = readOnly { db =>
    db.fromHistory(WEKeys.contractHistory(contractId.byteStr), WEKeys.contract(contractId.byteStr)).flatten
  }

  override def contractData(contractId: ByteStr, readingContext: ContractReadingContext): ExecutedContractData = readOnly { db =>
    ExecutedContractData((for {
      key   <- WEKeys.contractKeys(contractId, storage).members(db)
      value <- contractKeyData(db, contractId, key)
    } yield key -> value).toMap)
  }

  override def contractKeys(request: KeysRequest, readingContext: ContractReadingContext): Vector[String] = readOnly { db =>
    val KeysRequest(contractId, offsetOpt, limitOpt, keysFilter, knownKeys) = request

    val keys       = WEKeys.contractKeys(contractId, storage).members(db)
    val pagination = new KeysPagination((keys ++ knownKeys).iterator)
    pagination.paginatedKeys(offsetOpt, limitOpt, keysFilter).toVector
  }

  override def contractData(contractId: ByteStr, keys: Iterable[String], readingContext: ContractReadingContext): ExecutedContractData = readOnly {
    db =>
      ExecutedContractData((for {
        key   <- keys
        value <- contractKeyData(db, contractId, key)
      } yield key -> value).toMap)
  }

  override def contractData(contractId: ByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] = readOnly { db =>
    contractKeyData(db, contractId, key)
  }

  private def contractKeyData(db: MainReadOnlyDB, contractId: ByteStr, key: String): Option[DataEntry[_]] = {
    db.fromHistory(WEKeys.contractDataHistory(contractId, key), WEKeys.contractData(contractId, key)).flatten
  }

  override def executedTxFor(forTxId: AssetId): Option[ExecutedContractTransaction] = readOnly { db =>
    db.get(WEKeys.executedTxIdFor(forTxId)).flatMap(transactionInfo).map(_._2).map(_.asInstanceOf[ExecutedContractTransaction])
  }

  override def hasExecutedTxFor(forTxId: ByteStr): Boolean = readOnly { db =>
    db.has(WEKeys.executedTxIdFor(forTxId))
  }

  override def policies(): Set[ByteStr] = policyIdsSet.members

  override def policyExists(policyId: ByteStr): Boolean = policyIdsSet.contains(policyId)

  def policyOwners(policyId: ByteStr): Set[Address] =
    WEKeys.policyOwners(storage, policyId).members

  def policyRecipients(policyId: ByteStr): Set[Address] =
    WEKeys.policyRecipients(storage, policyId).members

  private def updatePolicy(rw: MainReadWriteDB, policyId: ByteStr, policyDiff: PolicyDiffValue): Unit = {
    WEKeys
      .policyOwners(storage, policyId)
      .addAndRemoveDisjoint(rw, policyDiff.ownersToAdd, policyDiff.ownersToRemove)

    WEKeys
      .policyRecipients(storage, policyId)
      .addAndRemoveDisjoint(rw, policyDiff.recipientsToAdd, policyDiff.recipientsToRemove)
  }

  def policyDataHashes(policyId: ByteStr): Set[PolicyDataHash] =
    WEKeys.policyDataHashes(storage, policyId).members

  override def policyDataHashExists(policyId: ByteStr, dataHash: PolicyDataHash): Boolean = {
    policyDataHashes(policyId).contains(dataHash)
  }

  private def updatePolicyDataHashes(rw: MainReadWriteDB,
                                     policyId: ByteStr,
                                     newDataHashesWithIds: Set[PolicyDataHashTransaction],
                                     opType: OpType): Unit = {
    val policyHashSet = WEKeys.policyDataHashes(storage, policyId)
    val newDataHashes = newDataHashesWithIds.map(_.dataHash)

    opType match {
      case OpType.Add =>
        policyHashSet.add(rw, newDataHashes)

        newDataHashesWithIds.foreach { tx =>
          rw.put(WEKeys.policyDataHashTxId(PolicyDataId(policyId, tx.dataHash)), Some(tx.id()))
        }
      case OpType.Remove =>
        policyHashSet.remove(rw, newDataHashes)

        newDataHashesWithIds.foreach { tx =>
          rw.delete(WEKeys.policyDataHashTxId(PolicyDataId(policyId, tx.dataHash)))
        }
    }
  }

  private[database] def loadBlock(height: Int): Option[Block] = readOnly { db =>
    loadBlock(height, db)
  }

  private[database] def loadBlock(height: Int, db: MainReadOnlyDB): Option[Block] = {
    val headerKey = Keys.blockHeaderAndSizeAt(height)
    for {
      (header, _) <- db.get(headerKey)
      blockTxs = db.get(Keys.blockTransactionsAtHeight(height)).map(transactionInfo(_, db)).collect {
        case Some((_, tx)) => tx
      }
      block = Block.build(header, blockTxs).explicitGet()
    } yield block
  }

  private[database] def readBlockTransactionBytes(h: Int, db: MainReadOnlyDB): Array[Byte] = {
    val out      = newDataOutput()
    val txIdList = db.get(Keys.blockTransactionsAtHeight(h))
    for (txId <- txIdList) {
      db.get(Keys.transactionBytes(txId))
        .map { txBytes =>
          out.writeInt(txBytes.length)
          out.write(txBytes)
        }
        .getOrElse(throw new RuntimeException(s"Cannot parse transaction with id '$txId' in block at height: $h"))
    }
    out.toByteArray
  }

  def allNonEmptyRoleAddresses: Stream[Address] = {
    def byInterval(from: BigInt, to: BigInt): Stream[Address] = {
      if (to >= from) {
        val head = readOnly(_.get(WEKeys.idToNonEmptyRoleAddress(from)))
        Stream.cons(head, byInterval(from + 1, to))
      } else {
        Stream.empty
      }
    }

    byInterval(1, loadMaxNonEmptyRoleAddressId())
  }

  private def updateMinerBanHistory(rw: MainReadWriteDB, address: Address, minerBanHistory: MinerBanHistory, targetHeight: Int): Unit = {
    val addressId = minerAddressId(address)
    updateMinerBanHistory(rw, addressId, minerBanHistory, targetHeight)
  }

  private def updateMinerBanHistory(rw: MainReadWriteDB, addressId: BigInt, minerBanHistory: MinerBanHistory, targetHeight: Int): Unit = {
    if (activatedFeatures.get(BlockchainFeature.MinerBanHistoryOptimisationFix.id).exists(_ <= targetHeight)) {
      val oldSize = lastMinerBanHistoryV2EntryId(addressId)
      val newSize = BigInt(minerBanHistory.size)

      log.trace(s"Update miner-ban-history-v2 for address-id '$addressId' with old size '$oldSize' and new size '$newSize'")

      // Update last id
      rw.put(WEKeys.lastMinerBanHistoryV2EntryId(addressId), Some(newSize))

      // Update new entries
      @tailrec
      def update(currentId: BigInt, rest: Seq[MinerBanlistEntry], unsavedEntriesCount: Int): Unit = {
        rest.headOption.foreach { entry =>
          log.trace(s"Update miner-ban-history-v2 entry '$entry' for address-id '$addressId' and entry-id '$currentId'")
          rw.put(WEKeys.idToMinerBanHistoryV2Entry(currentId, addressId), entry)
        }

        val nextUnsavedEntriesCount = unsavedEntriesCount - 1
        if (nextUnsavedEntriesCount > 0) {
          update(currentId - 1, rest.tail, nextUnsavedEntriesCount)
        }
      }

      if (minerBanHistory.unsavedDepth > 0) update(newSize, minerBanHistory.entries, minerBanHistory.unsavedDepth)

      // Clear old entries
      (newSize + 1 to oldSize).foreach { entryId =>
        log.trace(s"Clear miner-ban-history-v2 entry for address-id '$addressId' and entry-id '$entryId'")
        rw.delete(WEKeys.idToMinerBanHistoryV2Entry(entryId, addressId))
      }
    } else {
      rw.put(WEKeys.minerBanHistoryV1(addressId), Some(minerBanHistory.entries))
      // Remove v2 metadata for the case when we rolled back to v1 from v2
      rw.delete(WEKeys.lastMinerBanHistoryV2EntryId(addressId))
    }
  }

  private def updateMinerCancelledWarnings(rw: MainReadWriteDB)(address: Address, warnings: Seq[CancelledWarning]): Unit = {
    val minerId                  = minerAddressId(address)
    val trimmedCancelledWarnings = warnings.take(maxRollbackDepth)
    rw.put(WEKeys.minerCancelledWarnings(minerId), Some(trimmedCancelledWarnings))
  }

  private def minerAddressId(address: Address): BigInt = {
    addressId(address) match {
      case None =>
        throw new IllegalStateException(
          s"Didn't find ID for '$address' in the state: an exceptional case, because if the address is a miner, then it must already have an addressId in the state")
      case Some(minerId) => minerId
    }
  }

  private[this] val pendingPrivacyItemsSet = WEKeys.pendingPrivacyItemsSet(storage)
  private[this] val lostPrivacyItemsSet    = WEKeys.lostPrivacyItemsSet(storage)

  override def pendingPrivacyItems(): Set[PolicyDataId] =
    pendingPrivacyItemsSet.members

  override def isPending(policyId: ByteStr, dataHash: PolicyDataHash): Boolean =
    pendingPrivacyItemsSet.contains(PolicyDataId(policyId, dataHash))

  override def addToPending(policyId: ByteStr, dataHash: PolicyDataHash): Boolean =
    pendingPrivacyItemsSet.add(PolicyDataId(policyId, dataHash))

  override def addToPending(policyDataIds: Set[PolicyDataId]): Int =
    pendingPrivacyItemsSet.add(policyDataIds)

  override def removeFromPendingAndLost(policyId: ByteStr, dataHash: PolicyDataHash): (Boolean, Boolean) = readWrite { rw =>
    val dataId = PolicyDataId(policyId, dataHash)

    pendingPrivacyItemsSet.remove(rw, dataId) ->
      lostPrivacyItemsSet.remove(rw, dataId)
  }

  override def lostPrivacyItems(): Set[PolicyDataId] = lostPrivacyItemsSet.members

  override def isLost(policyId: ByteStr, dataHash: PolicyDataHash): Boolean =
    lostPrivacyItemsSet.contains(PolicyDataId(policyId, dataHash))

  override def pendingToLost(policyId: AssetId, dataHash: PolicyDataHash): (Boolean, Boolean) = readWrite { rw =>
    val dataId = PolicyDataId(policyId, dataHash)

    pendingPrivacyItemsSet.remove(rw, dataId) ->
      lostPrivacyItemsSet.add(rw, dataId)
  }

  override def policyDataHashTxId(id: PolicyDataId): Option[ByteStr] = readOnly { ro =>
    ro.get(WEKeys.policyDataHashTxId(id))
  }

  override def privacyItemDescriptor(policyId: ByteStr, dataHash: PolicyDataHash): Option[PrivacyItemDescriptor] = readOnly { ro =>
    ro.get(WEKeys.policyItemDescriptor(policyId, dataHash))
  }

  override def putItemDescriptor(policyId: ByteStr, dataHash: PolicyDataHash, descriptor: PrivacyItemDescriptor): Unit = readWrite { rw =>
    rw.put(WEKeys.policyItemDescriptor(policyId, dataHash), Some(descriptor))
  }

  protected[database] override def putCert(rw: MainReadWriteDB, cert: X509Certificate): Unit = {
    val distinguishedName = cert.getSubjectX500Principal.getName

    val dnHash      = ByteStr(DigestUtils.sha1(distinguishedName))
    val fingerprint = ByteStr(DigestUtils.sha1(cert.getEncoded))
    val pka         = PublicKeyAccount(PublicKey.fromJavaPk(cert.getPublicKey))

    rw.put(WEKeys.certByDnHash(dnHash), Some(cert))
    rw.put(WEKeys.certDnHashByPublicKey(pka), Some(dnHash))
    rw.put(WEKeys.certDnHashByFingerprint(fingerprint), Some(dnHash))
  }

  override def certByDistinguishedNameHash(dnHash: String): Option[Certificate] = {
    readOnly(_.get(WEKeys.certByDnHash(ByteStr(Hex.decodeHex(dnHash)))))
  }

  override def certByDistinguishedName(distinguishedName: String): Option[Certificate] = {
    val dnHash = ByteStr(DigestUtils.sha1(distinguishedName))
    readOnly(_.get(WEKeys.certByDnHash(dnHash)))
  }

  override def certByPublicKey(publicKey: PublicKeyAccount): Option[Certificate] = readOnly { ro =>
    for {
      dnHash <- ro.get(WEKeys.certDnHashByPublicKey(publicKey))
      cert   <- ro.get(WEKeys.certByDnHash(dnHash))
    } yield cert
  }

  override def certByFingerPrint(fingerprint: ByteStr): Option[Certificate] = readOnly { ro =>
    for {
      dnHash <- ro.get(WEKeys.certDnHashByFingerprint(fingerprint))
      cert   <- ro.get(WEKeys.certByDnHash(dnHash))
    } yield cert
  }

  override def certsAtHeight(height: Int): Set[Certificate] = readOnly { ro =>
    for {
      hash <- ro.get(WEKeys.certDnHashesAtHeight(height))
      cert <- ro.get(WEKeys.certByDnHash(hash))
    } yield cert
  }

  override protected[database] def putCertsAtHeight(rw: MainReadWriteDB, height: Int, certs: Set[X509Certificate]): Unit = {
    if (certs.nonEmpty) {
      val hashes = certs.map(cert => ByteStr(DigestUtils.sha1(cert.getSubjectX500Principal.getName)))
      rw.put(WEKeys.certDnHashesAtHeight(height), hashes)
    }
  }

  override def aliasesIssuedByAddress(address: Address): Set[Alias] = readOnly { ro =>
    ro.get(Keys.addressId(address)).fold(Set.empty[Alias])(id => Keys.issuedAliasesByAddressId(id, storage).members)
  }

  override protected[database] def putCrl(rw: MainReadWriteDB, publicKeyAccount: PublicKeyAccount, cdp: URL, crl: X509CRL, crlHash: ByteStr): Unit = {
    putCrlOnly(rw, publicKeyAccount, cdp, crl, crlHash)
    putCrlIssuers(Set(publicKeyAccount))
    putCrlUrlsForIssuerPublicKey(publicKeyAccount, Set(cdp))
  }

  private def putCrlOnly(rw: MainReadWriteDB, publicKeyAccount: PublicKeyAccount, cdp: URL, crl: X509CRL, crlHash: ByteStr): Unit = {
    val crlTimestamp = crl.getThisUpdate.getTime
    val crlKey       = CrlKey(publicKeyAccount, cdp, crlTimestamp)
    val crlData      = CrlData(crl, publicKeyAccount, cdp)

    rw.put(WEKeys.crlHashByKey(crlKey), Some(crlHash))
    rw.put(WEKeys.crlDataByHash(crlHash), Some(crlData))
  }

  def crlIssuers(): Set[PublicKeyAccount] = crlIssuersSet.members

  override def putCrlIssuers(issuers: Set[PublicKeyAccount]): Unit = crlIssuersSet.add(issuers)

  override def putCrlUrlsForIssuerPublicKey(publicKey: PublicKeyAccount, crlUrls: Set[URL]): Unit = {
    WEKeys.crlUrlsByIssuerPublicKey(publicKey, storage).add(crlUrls)
  }

  private def putCrlData(rw: MainReadWriteDB, crlHashesByIssuer: Map[PublicKeyAccount, Set[ByteStr]], crlDataByHash: Map[ByteStr, CrlData]): Unit = {
    crlHashesByIssuer.foreach {
      case (issuer, hashes) =>
        hashes.foreach { hash =>
          val crlData = crlDataByHash(hash)
          putCrlOnly(rw, issuer, crlData.cdp, crlData.crl, hash)
          putCrlUrlsForIssuerPublicKey(issuer, Set(crlData.cdp))
        }
    }
    putCrlIssuers(crlHashesByIssuer.keySet)
  }

  def putCrlDataByIssuer(crlDataByIssuer: Map[PublicKeyAccount, Set[CrlData]]): Unit = readWrite { rw =>
    crlDataByIssuer.foreach {
      case (issuer, crlDatas) =>
        crlDatas.foreach { crlData =>
          putCrlOnly(rw, issuer, crlData.cdp, crlData.crl, crlData.crlHash())
        }
        putCrlUrlsForIssuerPublicKey(issuer, crlDatas.map(_.cdp))
    }
    putCrlIssuers(crlDataByIssuer.keySet)
  }

  override def crlDataByHash(hash: ByteStr): Option[CrlData] =
    readOnly(_.get(WEKeys.crlDataByHash(hash)))

  def crlHashByKey(crlKey: CrlKey): Option[ByteStr] =
    readOnly(_.get(WEKeys.crlHashByKey(crlKey)))

  def crlUrlsByIssuerPublicKey(publicKey: PublicKeyAccount): Set[URL] = WEKeys.crlUrlsByIssuerPublicKey(publicKey, storage).members

  def actualCrls(issuer: PublicKeyAccount, fromTimestamp: Long, toTimestamp: Long): Set[CrlData] = readOnly { ro =>
    @tailrec
    def collectCrls(iterator: RocksIterator, prefix: Array[Byte], acc: Set[CrlData] = Set.empty): Set[CrlData] = {
      if (iterator.isValid && iterator.key().startsWith(prefix)) {
        val crlHash = ByteStr(iterator.value())
        ro.get(WEKeys.crlDataByHash(crlHash)) match {
          case Some(crlData) =>
            if (fromTimestamp < crlData.crl.getNextUpdate.getTime && crlData.crl.getThisUpdate.getTime <= toTimestamp) {
              iterator.prev()
              collectCrls(iterator, prefix, acc + crlData)
            } else {
              acc
            }
          case None =>
            throw new IllegalStateException(s"Corrupted database state. Missing CRL with hash '${crlHash.base58}'")
        }
      } else {
        acc
      }
    }

    val timestampBytes = Longs.toByteArray(toTimestamp)
    crlUrlsByIssuerPublicKey(issuer).flatMap { url =>
      val urlBytes = DigestUtils.sha1(url.toString)
      val prefix   = Array.concat(Shorts.toByteArray(CrlByKeyPrefix), issuer.publicKey.getEncoded, urlBytes)
      val target   = Array.concat(prefix, timestampBytes)
      val iterator = ro.iterator(CertsCF)

      iterator.seekForPrev(target)
      collectCrls(iterator, prefix)
    }
  }

  def lastCrl(issuer: PublicKeyAccount, cdp: URL, timestamp: Long): Option[X509CRL] = readOnly { ro =>
    @tailrec
    def collectLastCrl(iterator: RocksIterator, prefix: Array[Byte]): Option[X509CRL] = {
      if (iterator.isValid && iterator.key().startsWith(prefix)) {
        val crlHash = ByteStr(iterator.value())
        Some(
          ro.get(WEKeys.crlDataByHash(crlHash))
            .map(_.crl)
            .getOrElse(throw new IllegalStateException(s"Corrupted database state. Missing CRL with hash '${crlHash.base58}'")))
      } else if (!iterator.isValid) {
        None
      } else { iterator.prev(); collectLastCrl(iterator, prefix) }
    }

    crlUrlsByIssuerPublicKey(issuer)
      .collectFirst {
        case url if url == cdp =>
          val iterator = ro.iterator(CertsCF)
          iterator
      }
      .flatMap { iterator =>
        val urlBytes       = DigestUtils.sha1(cdp.toString)
        val prefix         = Shorts.toByteArray(CrlByKeyPrefix) ++ issuer.publicKey.getEncoded ++ urlBytes
        val timestampBytes = Longs.toByteArray(timestamp)
        val target         = prefix ++ timestampBytes
        iterator.seekForPrev(target)
        collectLastCrl(iterator, prefix)
      }
  }

  override def actualCrls(issuer: PublicKeyAccount, timestamp: Long): Set[CrlData] = actualCrls(issuer, timestamp, timestamp)
}
