package com.wavesenterprise.state.reader

import com.wavesenterprise.account.{Address, Alias, PublicKeyAccount}
import com.wavesenterprise.acl.Permissions
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, BlockHeader}
import com.wavesenterprise.consensus._
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId}
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.docker.{ExecutedContractData, ExecutedContractTransaction}
import com.wavesenterprise.transaction.lease.LeaseTransaction
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.{AssetId, Transaction, ValidationError}
import com.wavesenterprise.utils.ReadWriteLocking
import com.wavesenterprise.utils.pki.CrlData

import java.security.cert.{Certificate, X509Certificate}
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

trait ReadWriteLockingBlockchain extends Blockchain with ReadWriteLocking {

  override protected val lock: ReadWriteLock = new ReentrantReadWriteLock()

  protected[this] def state: Blockchain

  override def height: Int = readLock { state.height }

  override def score: BigInt = readLock { state.score }

  override def scoreOf(blockId: ByteStr): Option[BigInt] = readLock { state.scoreOf(blockId) }

  override def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = readLock { state.blockHeaderAndSize(height) }

  override def blockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)] = readLock { state.blockHeaderAndSize(blockId) }

  override def lastBlock: Option[Block] = readLock { state.lastBlock }

  override def lastPersistenceBlock: Option[Block] = readLock { state.lastPersistenceBlock }

  override def carryFee: Long = readLock { state.carryFee }

  override def blockBytes(height: Int): Option[Array[Byte]] = readLock { state.blockBytes(height) }

  override def blockBytes(blockId: ByteStr): Option[Array[Byte]] = readLock { state.blockBytes(blockId) }

  override def heightOf(blockId: ByteStr): Option[Int] = readLock { state.heightOf(blockId) }

  override def lastBlockIds(howMany: Int): Seq[ByteStr] = readLock { state.lastBlockIds(howMany) }

  override def lastBlockIds(startBlock: BlockId, howMany: Int): Option[Seq[BlockId]] = readLock { state.lastBlockIds(startBlock, howMany) }

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int): Option[Seq[ByteStr]] = readLock {
    state.blockIdsAfter(parentSignature, howMany)
  }

  override def parent(block: Block, back: Int): Option[Block] = readLock { state.parent(block, back) }

  override def parentHeader(block: Block): Option[BlockHeader] = readLock { state.parentHeader(block) }

  override def approvedFeatures: Map[Short, Int] = readLock { state.activatedFeatures }

  override def activatedFeatures: Map[Short, Int] = readLock { state.activatedFeatures }

  override def featureVotes(height: Int): Map[Short, Int] = readLock { state.featureVotes(height) }

  override def addressPortfolio(a: Address): Portfolio = readLock { state.addressPortfolio(a) }

  override def contractPortfolio(contractId: ContractId): Portfolio = readLock { state.contractPortfolio(contractId) }

  override def transactionInfo(id: ByteStr): Option[(Int, Transaction)] = readLock { state.transactionInfo(id) }

  override def transactionHeight(id: ByteStr): Option[Int] = readLock { state.transactionHeight(id) }

  override def addressTransactions(address: Address,
                                   txTypes: Set[Transaction.Type],
                                   count: Int,
                                   fromId: Option[ByteStr]): Either[String, Seq[(Int, Transaction)]] =
    readLock { state.addressTransactions(address, txTypes, count, fromId) }

  override def containsTransaction(tx: Transaction): Boolean = readLock { state.containsTransaction(tx) }

  override def containsTransaction(id: ByteStr): Boolean = readLock { state.containsTransaction(id) }

  override def assets(): Set[AssetId] = readLock { state.assets() }

  override def asset(id: AssetId): Option[AssetInfo] = readLock { state.asset(id) }

  override def assetDescription(id: ByteStr): Option[AssetDescription] = readLock { state.assetDescription(id) }

  override def resolveAlias(a: Alias): Either[ValidationError, Address] = readLock { state.resolveAlias(a) }

  override def leaseDetails(leaseId: ByteStr): Option[LeaseDetails] = readLock { state.leaseDetails(leaseId) }

  override def filledVolumeAndFee(orderId: ByteStr): VolumeAndFee = readLock { state.filledVolumeAndFee(orderId) }

  override def addressBalanceSnapshots(address: Address, from: Int, to: Int): Seq[BalanceSnapshot] = readLock {
    state.addressBalanceSnapshots(address, from, to)
  }

  override def contractBalanceSnapshots(contractId: ContractId, from: Int, to: Int): Seq[BalanceSnapshot] = readLock {
    state.contractBalanceSnapshots(contractId, from, to)
  }

  override def accounts(): Set[Address] = readLock { state.accounts() }

  override def accountScript(address: Address): Option[Script] = readLock { state.accountScript(address) }

  override def hasScript(address: Address): Boolean = readLock { state.hasScript(address) }

  override def assetScript(id: ByteStr): Option[Script] = readLock { state.assetScript(id) }

  override def hasAssetScript(id: ByteStr): Boolean = readLock { state.hasAssetScript(id) }

  override def accountDataSlice(acc: Address, from: Int, to: Int): AccountDataInfo = readLock { state.accountDataSlice(acc, from, to) }

  override def accountData(acc: Address): AccountDataInfo = readLock { state.accountData(acc) }

  override def accountData(acc: Address, key: String): Option[DataEntry[_]] = readLock { state.accountData(acc, key) }

  override def addressLeaseBalance(address: Address): LeaseBalance = readLock {
    state.addressLeaseBalance(address)
  }

  override def addressBalance(address: Address, mayBeAssetId: Option[AssetId]): Long = readLock {
    state.addressBalance(address, mayBeAssetId)
  }

  override def contractBalance(contractId: ContractId, mayBeAssetId: Option[AssetId], readingContext: ContractReadingContext): Long = readLock {
    state.contractBalance(contractId, mayBeAssetId, readingContext)
  }

  override def addressAssetDistribution(assetId: ByteStr): AssetDistribution = readLock {
    state.addressAssetDistribution(assetId)
  }

  override def addressAssetDistributionAtHeight(assetId: AssetId,
                                                height: Int,
                                                count: Int,
                                                fromAddress: Option[Address]): Either[ValidationError, AssetDistributionPage] = readLock {
    state.addressAssetDistributionAtHeight(assetId, height, count, fromAddress)
  }

  override def addressWestDistribution(height: Int): Map[Address, Long] = readLock {
    state.addressWestDistribution(height)
  }

  override def allActiveLeases: Set[LeaseTransaction] = readLock {
    state.allActiveLeases
  }

  override def collectAddressLposPortfolios[A](pf: PartialFunction[(Address, Portfolio), A]): Map[Address, A] = readLock {
    state.collectAddressLposPortfolios(pf)
  }

  override def append(
      diff: Diff,
      carryFee: Long,
      block: Block,
      consensusPostActionDiff: ConsensusPostActionDiff,
      certificates: Set[X509Certificate]
  ): Unit = readLock {
    state.append(diff, carryFee, block, consensusPostActionDiff, certificates)
  }

  override def rollbackTo(targetBlockId: ByteStr): Either[String, Seq[Block]] = readLock { state.rollbackTo(targetBlockId) }

  override def permissions(acc: Address): Permissions = readLock { state.permissions(acc) }

  override def miners: MinerQueue = readLock { state.miners }

  override def contractValidators: ContractValidatorPool = readLock { state.contractValidators }

  override def lastBlockContractValidators: Set[Address] = readLock { state.lastBlockContractValidators }

  override def minerBanHistory(address: Address): MinerBanHistory = readLock { state.minerBanHistory(address) }

  override def bannedMiners(height: Int): Seq[Address] = readLock { state.bannedMiners(height) }

  override def warningsAndBans: Map[Address, Seq[MinerBanlistEntry]] = readLock { state.warningsAndBans }

  override def participantPubKey(address: Address): Option[PublicKeyAccount] = readLock { state.participantPubKey(address) }

  override def networkParticipants(): Seq[Address] = readLock { state.networkParticipants() }

  override def policies(): Set[ByteStr] = readLock { state.policies() }

  override def policyExists(policyId: ByteStr): Boolean = readLock { state.policyExists(policyId) }

  override def policyOwners(policyId: ByteStr): Set[Address] = readLock { state.policyOwners(policyId) }

  override def policyRecipients(policyId: ByteStr): Set[Address] = readLock { state.policyRecipients(policyId) }

  override def policyDataHashes(policyId: ByteStr): Set[PolicyDataHash] = readLock { state.policyDataHashes(policyId) }

  override def policyDataHashExists(policyId: ByteStr, dataHash: PolicyDataHash): Boolean = readLock {
    state.policyDataHashExists(policyId, dataHash)
  }

  override def policyDataHashTxId(id: PolicyDataId): Option[ByteStr] = readLock {
    state.policyDataHashTxId(id)
  }

  override def allNonEmptyRoleAddresses: Stream[Address] = readLock { state.allNonEmptyRoleAddresses }

  override def contracts(): Set[ContractInfo] = readLock { state.contracts() }

  override def contract(contractId: ContractId): Option[ContractInfo] = readLock { state.contract(contractId) }

  override def contractKeys(request: KeysRequest, readingContext: ContractReadingContext): Vector[String] = readLock {
    state.contractKeys(request, readingContext)
  }

  override def contractData(contractId: ByteStr, readingContext: ContractReadingContext): ExecutedContractData = readLock {
    state.contractData(contractId, readingContext)
  }

  override def contractData(contractId: ByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] = readLock {
    state.contractData(contractId, key, readingContext)
  }

  override def contractData(contractId: ByteStr, keys: Iterable[String], readingContext: ContractReadingContext): ExecutedContractData = readLock {
    state.contractData(contractId, keys, readingContext)
  }

  override def executedTxFor(forTxId: ByteStr): Option[ExecutedContractTransaction] = readLock { state.executedTxFor(forTxId) }

  override def hasExecutedTxFor(forTxId: ByteStr): Boolean = readLock { state.hasExecutedTxFor(forTxId) }

  override def certByDistinguishedNameHash(dnHash: String): Option[Certificate] = readLock {
    state.certByDistinguishedNameHash(dnHash)
  }

  override def certByDistinguishedName(distinguishedName: String): Option[Certificate] = readLock {
    state.certByDistinguishedName(distinguishedName)
  }

  override def certByPublicKey(publicKey: PublicKeyAccount): Option[Certificate] = readLock {
    state.certByPublicKey(publicKey)
  }

  override def certByFingerPrint(fingerprint: ByteStr): Option[Certificate] = readLock {
    state.certByFingerPrint(fingerprint)
  }

  override def certsAtHeight(height: Int): Set[Certificate] = readLock {
    state.certsAtHeight(height)
  }

  override def aliasesIssuedByAddress(address: Address): Set[Alias] = readLock {
    state.aliasesIssuedByAddress(address)
  }

  override def crlDataByHash(crlHash: ByteStr): Option[CrlData] = readLock {
    state.crlDataByHash(crlHash)
  }

  override def actualCrls(issuer: PublicKeyAccount, timestamp: Long): Set[CrlData] = readLock {
    state.actualCrls(issuer, timestamp)
  }
}
