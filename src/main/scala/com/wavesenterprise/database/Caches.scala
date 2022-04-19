package com.wavesenterprise.database

import cats.implicits._
import com.google.common.cache.LoadingCache
import com.wavesenterprise.account.{Address, Alias, PublicKeyAccount}
import com.wavesenterprise.acl.{NonEmptyRole, OpType, Permissions}
import com.wavesenterprise.block.{Block, BlockHeader}
import com.wavesenterprise.consensus.MinerBanlistEntry.CancelledWarning
import com.wavesenterprise.consensus.{ConsensusPostActionDiff, MinerBanHistory}
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.docker.ExecutedContractData
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.{AssetId, PolicyDataHashTransaction, Transaction}
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.utils.Caches.cache

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

  private val leaseBalanceCache: LoadingCache[Address, LeaseBalance] = cache(maxCacheSize, loadLeaseBalance)
  protected def loadLeaseBalance(address: Address): LeaseBalance
  protected def discardLeaseBalance(address: Address): Unit = leaseBalanceCache.invalidate(address)
  override def leaseBalance(address: Address): LeaseBalance = leaseBalanceCache.get(address)

  private val portfolioCache: LoadingCache[Address, Portfolio] = cache(maxCacheSize, loadPortfolio)
  protected def loadPortfolio(address: Address): Portfolio
  protected def discardPortfolio(address: Address): Unit = portfolioCache.invalidate(address)
  override def portfolio(a: Address): Portfolio          = portfolioCache.get(a)

  private val assetDescriptionCache: LoadingCache[AssetId, Option[AssetDescription]] = cache(maxCacheSize, loadAssetDescription)
  protected def loadAssetDescription(assetId: AssetId): Option[AssetDescription]
  protected def discardAssetDescription(assetId: AssetId): Unit             = assetDescriptionCache.invalidate(assetId)
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

  protected def invalidateMinerBanHistoryCache() = {
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

  protected def doAppend(block: Block,
                         carryFee: Long,
                         addresses: Map[Address, BigInt],
                         nonEmptyRoleAddresses: Map[Address, BigInt],
                         westBalances: Map[BigInt, Long],
                         assetBalances: Map[BigInt, Map[ByteStr, Long]],
                         leaseBalances: Map[BigInt, LeaseBalance],
                         leaseStates: Map[ByteStr, Boolean],
                         transactions: Seq[Transaction],
                         addressTransactions: Map[BigInt, List[(Int, ByteStr)]],
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
                         contracts: Map[ByteStr, ContractInfo],
                         contractsData: Map[ByteStr, ExecutedContractData],
                         executedTxMapping: Map[ByteStr, ByteStr],
                         policies: Map[ByteStr, PolicyDiff],
                         policiesDataHashes: Map[ByteStr, Set[PolicyDataHashTransaction]],
                         minersBanHistory: Map[Address, MinerBanHistory],
                         minersCancelledWarnings: Map[Address, Seq[CancelledWarning]]): Unit

  override def append(diff: Diff, carryFee: Long, block: Block, consensusPostActionDiff: ConsensusPostActionDiff): Unit = {
    val newHeight = current._1 + 1

    val newAddresses = Set.newBuilder[Address]
    newAddresses ++= diff.addresses.filter(addressIdCache.get(_).isEmpty)
    for {
      (_, _, addresses) <- diff.transactionsMap.values
      address           <- addresses
      if addressIdCache.get(address).isEmpty
    } yield {
      newAddresses += address
    }

    val newAddressIds = (for {
      (address, offset) <- newAddresses.result().zipWithIndex
    } yield address -> (lastAddressId + offset + 1)).toMap

    def addressId(address: Address): BigInt = (newAddressIds.get(address) orElse addressIdCache.get(address)).get

    lastAddressId += newAddressIds.size

    log.trace(s"CACHE newAddressIds = $newAddressIds")
    log.trace(s"CACHE lastAddressId = $lastAddressId")

    val westBalances         = Map.newBuilder[BigInt, Long]
    val assetBalances        = Map.newBuilder[BigInt, Map[ByteStr, Long]]
    val leaseBalances        = Map.newBuilder[BigInt, LeaseBalance]
    val updatedLeaseBalances = Map.newBuilder[Address, LeaseBalance]
    val newPortfolios        = Map.newBuilder[Address, Portfolio]

    for ((address, portfolioDiff) <- diff.portfolios) {
      val newPortfolio = portfolioCache.get(address).combine(portfolioDiff)
      if (portfolioDiff.balance != 0) {
        westBalances += addressId(address) -> newPortfolio.balance
      }

      if (portfolioDiff.lease != LeaseBalance.empty) {
        leaseBalances += addressId(address) -> newPortfolio.lease
        updatedLeaseBalances += address     -> newPortfolio.lease
      }

      if (portfolioDiff.assets.nonEmpty) {
        val newAssetBalances = for { (k, v) <- portfolioDiff.assets if v != 0 } yield k -> newPortfolio.assets(k)
        if (newAssetBalances.nonEmpty) {
          assetBalances += addressId(address) -> newAssetBalances
        }
      }

      newPortfolios += address -> newPortfolio
    }

    val newFills = for {
      (orderId, fillInfo) <- diff.orderFills
    } yield orderId -> volumeAndFeeCache.get(orderId).combine(fillInfo)

    val permissions = diff.permissions

    val newNonEmptyRoleAddressIds = extractNewNonEmptyRoleAddresses(permissions)

    val registrationsMap = diff.registrations
      .groupBy(_.opType)
      .mapValues(_.map(reg => addressId(reg.address) -> reg.pubKey))

    val minerIds =
      miners
        .update(permissions)
        .suggestedMiners
        .map(addressId)

    val newValidatorsPool = contractValidators.update(permissions)
    val validatorIds      = newValidatorsPool.suggestedValidators.toSeq.map(addressId)

    current = (newHeight, (current._2 + block.blockScore()), Some(block))

    val blockTimestamp          = block.timestamp
    val newContractValidatorSet = newValidatorsPool.currentValidatorSet(blockTimestamp)
    lastBlockContractValidatorsCache = Some(newContractValidatorSet)
    log.trace(s"Updated contract validators cache at '$blockTimestamp': '${newContractValidatorSet.mkString("', '")}'")

    doAppend(
      block = block,
      carryFee = carryFee,
      addresses = newAddressIds,
      nonEmptyRoleAddresses = newNonEmptyRoleAddressIds,
      westBalances = westBalances.result(),
      assetBalances = assetBalances.result(),
      leaseBalances = leaseBalances.result(),
      leaseStates = diff.leaseState,
      transactions = diff.transactions,
      addressTransactions = diff.accountTransactionIds.map({ case (addr, txs) => addressId(addr) -> txs }),
      assets = diff.assets,
      filledQuantity = newFills,
      scripts = diff.scripts.map { case (address, s) => addressId(address) -> s },
      assetScripts = diff.assetScripts,
      data = diff.accountData.map { case (address, data) => addressId(address) -> data },
      aliases = diff.aliases.map { case (a, address)     => a                  -> addressId(address) },
      sponsorship = diff.sponsorship,
      permissions = permissions.map { case (address, perms) => addressId(address) -> perms },
      registrations = registrationsMap,
      miners = minerIds,
      validators = validatorIds,
      contracts = diff.contracts,
      contractsData = diff.contractsData,
      executedTxMapping = diff.executedTxMapping,
      policies = diff.policies,
      policiesDataHashes = diff.policiesDataHashes,
      minersBanHistory = consensusPostActionDiff.minersBanHistory,
      minersCancelledWarnings = consensusPostActionDiff.cancelledWarnings
    )

    for ((address, id)           <- newAddressIds) addressIdCache.put(address, Some(id))
    for ((orderId, volumeAndFee) <- newFills) volumeAndFeeCache.put(orderId, volumeAndFee)
    for ((address, portfolio)    <- newPortfolios.result()) portfolioCache.put(address, portfolio)
    for (id                      <- diff.assets.keySet ++ diff.assetScripts.keySet ++ diff.sponsorship.keySet) assetDescriptionCache.invalidate(id)
    leaseBalanceCache.putAll(updatedLeaseBalances.result().asJava)
    scriptCache.putAll(diff.scripts.asJava)
    hasScriptCache.putAll(diff.scripts.mapValues(_.isDefined: java.lang.Boolean).asJava)
    assetScriptCache.putAll(diff.assetScripts.asJava)
    val newPermissionsCache = permissions.map { case (k, v) => addressId(k) -> v }
    permissionsCache.putAll(newPermissionsCache.asJava)
    invalidateMinerBanHistoryCache()
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

  override def rollbackTo(targetBlockId: ByteStr): Either[String, Seq[Block]] = {
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
      discardedBlocks
    }
  }
}
