package com.wavesenterprise.database

import cats.implicits._
import com.google.common.cache.LoadingCache
import com.wavesenterprise.account.{Address, Alias, PublicKeyAccount}
import com.wavesenterprise.acl.{NonEmptyRole, OpType, Permissions}
import com.wavesenterprise.block.{Block, BlockHeader}
import com.wavesenterprise.consensus.MinerBanlistEntry.CancelledWarning
import com.wavesenterprise.consensus.{ConsensusPostActionDiff, MinerBanHistory}
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state._
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.transaction.docker.ExecutedContractData
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.{AssetId, PolicyDataHashTransaction, Transaction}
import com.wavesenterprise.utils.Caches.cache
import com.wavesenterprise.utils.pki.CrlData
import com.wavesenterprise.utils.ScorexLogging

import java.security.cert.X509Certificate
import scala.collection.JavaConverters._

trait Caches extends Blockchain with ScorexLogging {

  protected def maxCacheSize: Int

  @volatile
  private var current = (loadHeight(), loadScore(), loadLastBlock())

  @volatile private var lastBlockContractValidatorsCache: Option[Set[Address]] = None

  protected def loadLastBlockContractValidators: Set[Address]

  override def lastBlockContractValidators: Set[Address] = {
    lastBlockContractValidatorsCache.getOrElse {
      val loadedValidators = loadLastBlockContractValidators
      log.trace(s"Loaded contract validators cache: '${loadedValidators.mkString("', '")}'")
      lastBlockContractValidatorsCache = Some(loadedValidators)
      loadedValidators
    }
  }

  protected def loadHeight(): Int
  override def height: Int = current._1

  protected def safeRollbackHeight: Int

  protected def loadScore(): BigInt
  override def score: BigInt = current._2

  protected def loadLastBlock(): Option[Block]
  override def lastBlock: Option[Block]            = current._3
  override def lastPersistenceBlock: Option[Block] = lastBlock

  def loadScoreOf(blockId: ByteStr): Option[BigInt]
  override def scoreOf(blockId: ByteStr): Option[BigInt] = {
    val c = current
    if (c._3.exists(_.uniqueId == blockId)) {
      Some(c._2)
    } else {
      loadScoreOf(blockId)
    }
  }

  def loadBlockHeaderAndSize(height: Int): Option[(BlockHeader, Int)]
  override def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = {
    val c = current
    if (height == c._1) {
      c._3.map(b => (b.blockHeader, b.bytes().length))
    } else {
      loadBlockHeaderAndSize(height)
    }
  }

  def loadBlockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)]
  override def blockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)] = {
    val c = current
    if (c._3.exists(_.uniqueId == blockId)) {
      c._3.map(b => (b.blockHeader, b.bytes().length))
    } else {
      loadBlockHeaderAndSize(blockId)
    }
  }

  def loadBlockBytes(height: Int): Option[Array[Byte]]
  override def blockBytes(height: Int): Option[Array[Byte]] = {
    val c = current
    if (height == c._1) {
      c._3.map(_.bytes())
    } else {
      loadBlockBytes(height)
    }
  }

  def loadBlockBytes(blockId: ByteStr): Option[Array[Byte]]
  override def blockBytes(blockId: ByteStr): Option[Array[Byte]] = {
    val c = current
    if (c._3.exists(_.uniqueId == blockId)) {
      c._3.map(_.bytes())
    } else {
      loadBlockBytes(blockId)
    }
  }

  def loadHeightOf(blockId: ByteStr): Option[Int]
  override def heightOf(blockId: ByteStr): Option[Int] = {
    val c = current
    if (c._3.exists(_.uniqueId == blockId)) {
      Some(c._1)
    } else {
      loadHeightOf(blockId)
    }
  }

  override def containsTransaction(tx: Transaction): Boolean = containsTransaction(tx.id())

  override def addressLeaseBalance(address: Address): LeaseBalance = addressLeaseBalanceCache.get(address)

  private val addressLeaseBalanceCache: LoadingCache[Address, LeaseBalance] = cache(maxCacheSize, loadAddressLeaseBalance)

  protected def loadAddressLeaseBalance(address: Address): LeaseBalance

  protected def discardAddressLeaseBalance(address: Address): Unit = addressLeaseBalanceCache.invalidate(address)

  override def contractLeaseBalance(address: ContractId): LeaseBalance = contractLeaseBalanceCache.get(address)

  private val contractLeaseBalanceCache: LoadingCache[ContractId, LeaseBalance] = cache(maxCacheSize, loadContractLeaseBalance)

  protected def loadContractLeaseBalance(contractId: ContractId): LeaseBalance

  protected def discardContractLeaseBalance(contractId: ContractId): Unit = contractLeaseBalanceCache.invalidate(contractId)

  private val addressPortfolioCache: LoadingCache[Address, Portfolio] = cache(maxCacheSize, loadAddressPortfolio)

  protected def loadAddressPortfolio(address: Address): Portfolio

  protected def discardAddressPortfolio(address: Address): Unit = addressPortfolioCache.invalidate(address)

  override def addressPortfolio(a: Address): Portfolio = addressPortfolioCache.get(a)

  private val contractPortfolioCache: LoadingCache[ContractId, Portfolio] = cache(maxCacheSize, loadContractPortfolio)

  protected def loadContractPortfolio(contractId: ContractId): Portfolio

  protected def discardContractPortfolio(contractId: ContractId): Unit = contractPortfolioCache.invalidate(contractId.byteStr)

  override def contractPortfolio(contractId: ContractId): Portfolio = contractPortfolioCache.get(contractId)

  private val assetDescriptionCache: LoadingCache[AssetId, Option[AssetDescription]] = cache(maxCacheSize, loadAssetDescription)

  protected def loadAssetDescription(assetId: AssetId): Option[AssetDescription]

  protected def discardAssetDescription(assetId: AssetId): Unit = assetDescriptionCache.invalidate(assetId)

  override def assetDescription(assetId: AssetId): Option[AssetDescription] = assetDescriptionCache.get(assetId)

  private val volumeAndFeeCache: LoadingCache[ByteStr, VolumeAndFee] = cache(maxCacheSize, loadVolumeAndFee)
  protected def loadVolumeAndFee(orderId: ByteStr): VolumeAndFee
  protected def discardVolumeAndFee(orderId: ByteStr): Unit       = volumeAndFeeCache.invalidate(orderId)
  override def filledVolumeAndFee(orderId: ByteStr): VolumeAndFee = volumeAndFeeCache.get(orderId)

  private val scriptCache: LoadingCache[Address, Option[Script]]       = cache(maxCacheSize, loadScript)
  private val hasScriptCache: LoadingCache[Address, java.lang.Boolean] = cache(maxCacheSize, hasScriptBytes)
  protected def loadScript(address: Address): Option[Script]
  protected def hasScriptBytes(address: Address): Boolean
  protected def discardScript(address: Address): Unit = {
    hasScriptCache.invalidate(address)
    scriptCache.invalidate(address)
  }

  override def accountScript(address: Address): Option[Script] = scriptCache.get(address)
  override def hasScript(address: Address): Boolean            = hasScriptCache.get(address)

  private val assetScriptCache: LoadingCache[AssetId, Option[Script]] = cache(maxCacheSize, loadAssetScript)
  protected def loadAssetScript(asset: AssetId): Option[Script]
  protected def hasAssetScriptBytes(asset: AssetId): Boolean
  protected def discardAssetScript(asset: AssetId): Unit = assetScriptCache.invalidate(asset)

  override def assetScript(asset: AssetId): Option[Script] = assetScriptCache.get(asset)
  override def hasAssetScript(asset: AssetId): Boolean =
    assetScriptCache.getIfPresent(asset) match {
      case null => hasAssetScriptBytes(asset)
      case x    => x.nonEmpty
    }

  protected def loadPermissions(addressId: BigInt): Permissions
  protected val permissionsCache: LoadingCache[BigInt, Permissions] = cache(maxCacheSize, loadPermissions)
  def permissions(addressId: BigInt): Permissions                   = permissionsCache.get(addressId)

  private var lastAddressId = loadMaxAddressId()
  protected def loadMaxAddressId(): BigInt

  private var lastContractStateId = loadMaxContractStateId()
  protected def loadMaxContractStateId(): BigInt

  private val contractStateIdCache: LoadingCache[ContractId, Option[BigInt]] =
    cache(maxCacheSize, loadStateIdByContractId)
  protected def loadStateIdByContractId(contractId: ContractId): Option[BigInt]
  protected def stateIdByContractId(contractId: ContractId): Option[BigInt] = contractStateIdCache.get(contractId)

  private val stateIdToContractIdCache: LoadingCache[BigInt, ByteStr] = cache(maxCacheSize, loadContractByStateId)
  protected def loadContractByStateId(stateId: BigInt): ByteStr

  protected def contractByStateId(stateId: BigInt): ByteStr = stateIdToContractIdCache.get(stateId)

  private val addressIdCache: LoadingCache[Address, Option[BigInt]] = cache(maxCacheSize, loadAddressId)
  protected def loadAddressId(address: Address): Option[BigInt]
  protected def addressId(address: Address): Option[BigInt] = addressIdCache.get(address)

  private val idToAddressCache: LoadingCache[BigInt, Address] = cache(maxCacheSize, loadAddressById)
  protected def loadAddressById(id: BigInt): Address
  protected def idToAddress(id: BigInt): Address = idToAddressCache.get(id)

  override def accounts(): Set[Address] = {
    (BigInt(1) to lastAddressId).flatMap { id =>
      Option(idToAddress(id))
    }.toSet
  }

  private var lastNonEmptyRoleAddressId = loadMaxNonEmptyRoleAddressId()
  protected def loadMaxNonEmptyRoleAddressId(): BigInt

  private val nonEmptyRoleAddressIdCache: LoadingCache[Address, Option[BigInt]] = cache(maxCacheSize, loadNonEmptyRoleAddressId)
  protected def loadNonEmptyRoleAddressId(address: Address): Option[BigInt]
  protected def nonEmptyRoleAddressId(address: Address): Option[BigInt] = nonEmptyRoleAddressIdCache.get(address)

  protected def loadMinerBanHistory(address: Address): MinerBanHistory
  private val minerBanHistoryCache: LoadingCache[Address, MinerBanHistory] = cache(maxCacheSize, loadMinerBanHistory)
  override def minerBanHistory(address: Address): MinerBanHistory          = minerBanHistoryCache.get(address)

  protected def loadLastMinerBanHistoryV2EntryId(addressId: BigInt): BigInt
  private val lastMinerBanEntryIdCache: LoadingCache[BigInt, BigInt]    = cache(maxCacheSize, loadLastMinerBanHistoryV2EntryId)
  protected def lastMinerBanHistoryV2EntryId(addressId: BigInt): BigInt = lastMinerBanEntryIdCache.get(addressId)
  protected def lastMinerBanHistoryV2EntryId(address: Address): BigInt  = addressId(address).fold(BigInt(0))(lastMinerBanHistoryV2EntryId)

  protected def invalidateMinerBanHistoryCache(): Unit = {
    minerBanHistoryCache.invalidateAll()
    lastMinerBanEntryIdCache.invalidateAll()
  }

  /**
    * Warning: Unsafe!
    * Don't call it from anywhere other than insertParticipant() method !!!
    */
  protected def nextFreeAddressId(): BigInt = {
    lastAddressId += 1
    lastAddressId
  }

  @volatile
  protected var approvedFeaturesCache: Map[Short, Int] = loadApprovedFeatures()
  protected def loadApprovedFeatures(): Map[Short, Int]
  override def approvedFeatures: Map[Short, Int] = approvedFeaturesCache

  @volatile
  protected var activatedFeaturesCache: Map[Short, Int] = loadActivatedFeatures()
  protected def loadActivatedFeatures(): Map[Short, Int]
  override def activatedFeatures: Map[Short, Int] = activatedFeaturesCache

  protected def contractsIdSet(): Set[ByteStr]

  protected def doAppend(
      block: Block,
      carryFee: Long,
      newAssetHoldersMap: Map[AssetHolder, BigInt],
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
      miners: Seq[BigInt],
      validators: Seq[BigInt],
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
  ): Unit

  override def append(
      diff: Diff,
      carryFee: Long,
      block: Block,
      consensusPostActionDiff: ConsensusPostActionDiff,
      certificates: Set[X509Certificate]
  ): Int = {
    val newHeight = current._1 + 1

    val (newAccountIds, newContractIds) = {
      val newAssetHolders = Set.newBuilder[AssetHolder]

      val newAddressesFromDiff = diff.addresses.filter(addressIdCache.get(_).isEmpty).map(_.toAssetHolder)
      val newContractsFromDiff = diff.contractIds.filter(contractStateIdCache.get(_).isEmpty).map(_.toAssetHolder)
      val newTransactionRecipientsFromDiff = diff.transactionsMap.values.foldLeft(Set.empty[AssetHolder]) {
        case (resultNewAssetHolders, (_, _, transactionAffectedAssetHolders)) =>
          resultNewAssetHolders ++ transactionAffectedAssetHolders.filter(
            _.product(addressIdCache.get(_).isEmpty, contractStateIdCache.get(_).isEmpty))
      }

      newAssetHolders ++= Seq.concat(newAddressesFromDiff, newContractsFromDiff, newTransactionRecipientsFromDiff)

      val (newAccounts, newContracts) = newAssetHolders
        .result()
        .partition(_.isInstanceOf[Account])

      ((for {
         (newAddressId, offset) <- newAccounts.collectAddresses.zipWithIndex
       } yield newAddressId -> (lastAddressId + offset + 1)).toMap.toAssetHolderMap,
       (for {
         (newContractStateId, offset) <- newContracts.collectContractIds.zipWithIndex
       } yield newContractStateId -> (lastContractStateId + offset + 1)).toMap.toAssetHolderMap)
    }

    val newAssetHolderIds = newAccountIds ++ newContractIds

    def getAddressId(address: Address): BigInt = (newAccountIds.get(address.toAssetHolder) orElse addressIdCache.get(address)).get

    def getContractStateId(contractId: ContractId): BigInt =
      (newContractIds.get(contractId.toAssetHolder) orElse contractStateIdCache.get(contractId)).get

    lastAddressId += newAccountIds.size
    lastContractStateId += newContractIds.size

    log.trace(s"CACHE newAssetHolderIds = $newAssetHolderIds")
    log.trace(s"CACHE lastAddressId = $lastAddressId")
    log.trace(s"CACHE lastContractStateId = $lastContractStateId")

    val westAccountBalances          = Map.newBuilder[BigInt, Long]
    val assetAccountBalances         = Map.newBuilder[BigInt, Map[ByteStr, Long]]
    val addressLeaseBalances         = Map.newBuilder[BigInt, LeaseBalance]
    val contractLeaseBalances        = Map.newBuilder[BigInt, LeaseBalance]
    val westContractBalances         = Map.newBuilder[BigInt, Long]
    val assetContractBalances        = Map.newBuilder[BigInt, Map[ByteStr, Long]]
    val updatedAddressLeaseBalances  = Map.newBuilder[Address, LeaseBalance]
    val updatedContractLeaseBalances = Map.newBuilder[ContractId, LeaseBalance]
    val newPortfolios                = Map.newBuilder[AssetHolder, Portfolio]

    for ((owner, portfolioDiff) <- diff.portfolios) {
      val newPortfolio = owner.product(addressPortfolioCache.get, contractPortfolioCache.get(_)).combine(portfolioDiff)
      owner.product(
        a => westAccountBalances += getAddressId(a) -> newPortfolio.balance,
        c => westContractBalances += getContractStateId(c) -> newPortfolio.balance
      )

      if (portfolioDiff.lease.nonEmpty) {
        owner match {
          case Account(address) =>
            addressLeaseBalances += getAddressId(address) -> newPortfolio.lease
            updatedAddressLeaseBalances += address        -> newPortfolio.lease
          case Contract(contractId) =>
            contractLeaseBalances += getContractStateId(contractId) -> newPortfolio.lease
            updatedContractLeaseBalances += contractId              -> newPortfolio.lease
        }
      }

      if (portfolioDiff.assets.nonEmpty) {
        val newAssetBalances = for { (k, v) <- portfolioDiff.assets if v != 0 } yield k -> newPortfolio.assets(k)
        if (newAssetBalances.nonEmpty) {
          owner.product(
            a => assetAccountBalances += getAddressId(a) -> newAssetBalances,
            c => assetContractBalances += getContractStateId(c) -> newAssetBalances
          )
        }
      }

      newPortfolios += owner -> newPortfolio
    }

    val newFills = for {
      (orderId, fillInfo) <- diff.orderFills
    } yield orderId -> volumeAndFeeCache.get(orderId).combine(fillInfo)

    val permissions = diff.permissions

    val newNonEmptyRoleAddressIds = extractNewNonEmptyRoleAddresses(permissions)

    val registrationsMap = diff.registrations
      .groupBy(_.opType)
      .mapValues(_.map(reg => getAddressId(reg.address) -> reg.pubKey))

    val minerIds =
      miners
        .update(permissions)
        .suggestedMiners
        .map(getAddressId)

    val newValidatorsPool = contractValidators.update(permissions)
    val validatorIds      = newValidatorsPool.suggestedValidators.toSeq.map(getAddressId)

    current = (newHeight, current._2 + block.blockScore(), Some(block))

    val blockTimestamp          = block.timestamp
    val newContractValidatorSet = newValidatorsPool.currentValidatorSet(blockTimestamp)
    lastBlockContractValidatorsCache = Some(newContractValidatorSet)
    log.trace(
      s"Updated contract validators cache at '$blockTimestamp': '${newContractValidatorSet.mkString("', '")}'.\n"
    )

    doAppend(
      block = block,
      carryFee = carryFee,
      newAssetHoldersMap = newAssetHolderIds,
      newNonEmptyRoleAddresses = newNonEmptyRoleAddressIds,
      westAddressesBalances = westAccountBalances.result(),
      assetAddressesBalances = assetAccountBalances.result(),
      addressLeaseBalances = addressLeaseBalances.result(),
      contractLeaseBalances = contractLeaseBalances.result(),
      westContractsBalances = westContractBalances.result(),
      assetContractsBalances = assetContractBalances.result(),
      leaseMap = diff.leaseMap,
      leaseCancelMap = diff.leaseCancelMap,
      transactions = diff.transactions,
      addressTransactions = diff.assetHolderTransactionIds.collectAddresses.map({
        case (aStateId, txs) => getAddressId(aStateId) -> txs
      }),
      contractTransactions = diff.assetHolderTransactionIds.collectContractIds.map({
        case (cStateId, txs) => getContractStateId(cStateId) -> txs
      }),
      assets = diff.assets,
      filledQuantity = newFills,
      scripts = diff.scripts.map { case (address, s) => getAddressId(address) -> s },
      assetScripts = diff.assetScripts,
      data = diff.accountData.map { case (address, data) => getAddressId(address) -> data },
      aliases = diff.aliases.map { case (a, address) => a -> getAddressId(address) },
      sponsorship = diff.sponsorship,
      permissions = permissions.map { case (address, perms) => getAddressId(address) -> perms },
      registrations = registrationsMap,
      miners = minerIds,
      validators = validatorIds,
      contracts = diff.contracts,
      contractsData = diff.contractsData,
      executedTxMapping = diff.executedTxMapping,
      policies = diff.policies,
      policiesDataHashes = diff.policiesDataHashes,
      minersBanHistory = consensusPostActionDiff.minersBanHistory,
      minersCancelledWarnings = consensusPostActionDiff.cancelledWarnings,
      certificates = certificates,
      crlDataByHash = diff.crlDataByHash,
      crlHashesByIssuer = diff.crlHashesByIssuer
    )

    for ((newAssetHolder, id) <- newAssetHolderIds)
      newAssetHolder.product(addressIdCache.put(_, Some(id)), contractId => contractStateIdCache.put(contractId, Some(id)))
    for ((orderId, volumeAndFee)  <- newFills) volumeAndFeeCache.put(orderId, volumeAndFee)
    for ((assetHolder, portfolio) <- newPortfolios.result())
      assetHolder.product(addressPortfolioCache.put(_, portfolio), contractPortfolioCache.put(_, portfolio))

    for (id <- diff.assets.keySet ++ diff.assetScripts.keySet ++ diff.sponsorship.keySet) assetDescriptionCache.invalidate(id)
    addressLeaseBalanceCache.putAll(updatedAddressLeaseBalances.result().asJava)
    contractLeaseBalanceCache.putAll(updatedContractLeaseBalances.result().asJava)
    scriptCache.putAll(diff.scripts.asJava)
    hasScriptCache.putAll(diff.scripts.mapValues(_.isDefined: java.lang.Boolean).asJava)
    assetScriptCache.putAll(diff.assetScripts.asJava)
    val newPermissionsCache = permissions.map { case (k, v) => getAddressId(k) -> v }
    permissionsCache.putAll(newPermissionsCache.asJava)
    invalidateMinerBanHistoryCache()
    newHeight
  }

  private def extractNewNonEmptyRoleAddresses(permissions: Map[Address, Permissions]): Map[Address, BigInt] = {
    val newNonEmptyRoleAddresses = permissions.collect {
      case (address, permissions) if nonEmptyRoleAddressIdCache.get(address).isEmpty && permissions.exists(_.role.isInstanceOf[NonEmptyRole]) =>
        address
    }

    val newNonEmptyRoleAddressIds = (for {
      (address, offset) <- newNonEmptyRoleAddresses.zipWithIndex
    } yield address -> (lastNonEmptyRoleAddressId + offset + 1)).toMap
    log.trace(s"CACHE newNonEmptyRoleAddressIds = $newNonEmptyRoleAddressIds")

    lastNonEmptyRoleAddressId += newNonEmptyRoleAddressIds.size
    log.trace(s"CACHE lastNonEmptyRoleAddressId = $lastNonEmptyRoleAddressId")

    newNonEmptyRoleAddressIds
  }

  protected def doRollback(targetBlockId: ByteStr): Seq[Block]

  override def rollbackTo(targetBlockId: ByteStr): Either[String, RollbackResult] = {
    for {
      height <- heightOf(targetBlockId)
        .toRight(s"No block with signature: $targetBlockId found in blockchain")
      _ <- Either
        .cond(
          height > safeRollbackHeight,
          (),
          s"Rollback is possible only to the block at a height: ${safeRollbackHeight + 1}"
        )
      discardedBlocks = doRollback(targetBlockId)
    } yield {
      current = (loadHeight(), loadScore(), loadLastBlock())
      lastBlockContractValidatorsCache = lastBlock.map(block => contractValidators.currentValidatorSet(block.timestamp))
      log.trace(s"Updated contract validators cache: ${lastBlockContractValidatorsCache.fold("-")(_.mkString("'", "', '", "'"))}")
      RollbackResult(height, discardedBlocks)
    }
  }
}
