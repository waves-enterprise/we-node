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

import java.security.cert.{Certificate, X509Certificate}

class DelegatingBlockchain(blockchain: Blockchain) extends Blockchain {

  @volatile private[this] var state: Blockchain = blockchain

  def setState(blockchain: Blockchain): Unit = {
    state = blockchain
  }

  override def height: Int = state.height

  override def score: BigInt = state.score

  override def scoreOf(blockId: ByteStr): Option[BigInt] = state.scoreOf(blockId)

  override def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = state.blockHeaderAndSize(height)

  override def blockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)] = state.blockHeaderAndSize(blockId)

  override def lastBlock: Option[Block] = state.lastBlock

  override def lastPersistenceBlock: Option[Block] = state.lastPersistenceBlock

  override def carryFee: Long = state.carryFee

  override def blockBytes(height: Int): Option[Array[Byte]] = state.blockBytes(height)

  override def blockBytes(blockId: ByteStr): Option[Array[Byte]] = state.blockBytes(blockId)

  override def heightOf(blockId: ByteStr): Option[Int] = state.heightOf(blockId)

  override def lastBlockIds(howMany: Int): Seq[ByteStr] = state.lastBlockIds(howMany)

  override def lastBlockIds(startBlock: BlockId, howMany: Int): Option[Seq[BlockId]] = state.lastBlockIds(startBlock, howMany)

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int): Option[Seq[ByteStr]] =
    state.blockIdsAfter(parentSignature, howMany)

  override def parent(block: Block, back: Int): Option[Block] = state.parent(block, back)

  override def parentHeader(block: Block): Option[BlockHeader] = state.parentHeader(block)

  override def approvedFeatures: Map[Short, Int] = state.activatedFeatures

  override def activatedFeatures: Map[Short, Int] = state.activatedFeatures

  override def featureVotes(height: Int): Map[Short, Int] = state.featureVotes(height)

  override def addressBalance(address: Address, mayBeAssetId: Option[AssetId]): Long = state.addressBalance(address, mayBeAssetId)

  override def addressPortfolio(a: Address): Portfolio = state.addressPortfolio(a)

  override def contractBalance(contractId: ByteStr, mayBeAssetId: Option[AssetId]): Long = state.contractBalance(contractId, mayBeAssetId)

  override def contractPortfolio(contractId: ByteStr): Portfolio = state.contractPortfolio(contractId)

  override def transactionInfo(id: ByteStr): Option[(Int, Transaction)] = state.transactionInfo(id)

  override def transactionHeight(id: ByteStr): Option[Int] = state.transactionHeight(id)

  override def addressTransactions(address: Address,
                                   txTypes: Set[Transaction.Type],
                                   count: Int,
                                   fromId: Option[AssetId]): Either[String, Seq[(Int, Transaction)]] =
    state.addressTransactions(address, txTypes, count, fromId)

  override def containsTransaction(tx: Transaction): Boolean = state.containsTransaction(tx)

  override def containsTransaction(id: ByteStr): Boolean = state.containsTransaction(id)

  override def assets(): Set[AssetId] = state.assets()

  override def asset(id: AssetId): Option[AssetInfo] = state.asset(id)

  override def assetDescription(id: ByteStr): Option[AssetDescription] = state.assetDescription(id)

  override def resolveAlias(a: Alias): Either[ValidationError, Address] = state.resolveAlias(a)

  override def addressLeaseBalance(address: Address): LeaseBalance = state.addressLeaseBalance(address)

  override def leaseDetails(leaseId: ByteStr): Option[LeaseDetails] = state.leaseDetails(leaseId)

  override def filledVolumeAndFee(orderId: ByteStr): VolumeAndFee = state.filledVolumeAndFee(orderId)

  override def addressBalanceSnapshots(address: Address, from: Int, to: Int): Seq[BalanceSnapshot] = state.addressBalanceSnapshots(address, from, to)

  override def contractBalanceSnapshots(contractId: ByteStr, from: Int, to: Int): Seq[BalanceSnapshot] =
    state.contractBalanceSnapshots(contractId, from, to)

  override def accounts(): Set[Address] = state.accounts()

  override def accountScript(address: Address): Option[Script] = state.accountScript(address)

  override def hasScript(address: Address): Boolean = state.hasScript(address)

  override def assetScript(id: ByteStr): Option[Script] = state.assetScript(id)

  override def hasAssetScript(id: ByteStr): Boolean = state.hasAssetScript(id)

  override def accountDataSlice(acc: Address, from: Int, to: Int): AccountDataInfo = state.accountDataSlice(acc, from, to)

  override def accountData(acc: Address): AccountDataInfo = state.accountData(acc)

  override def accountData(acc: Address, key: String): Option[DataEntry[_]] = state.accountData(acc, key)

  override def addressAssetDistribution(assetId: ByteStr): AssetDistribution = state.addressAssetDistribution(assetId)

  override def addressAssetDistributionAtHeight(assetId: AssetId,
                                                height: Int,
                                                count: Int,
                                                fromAddress: Option[Address]): Either[ValidationError, AssetDistributionPage] =
    state.addressAssetDistributionAtHeight(assetId, height, count, fromAddress)

  override def addressWestDistribution(height: Int): Map[Address, Long] = state.addressWestDistribution(height)

  override def allActiveLeases: Set[LeaseTransaction] = state.allActiveLeases

  override def collectAddressLposPortfolios[A](pf: PartialFunction[(Address, Portfolio), A]): Map[Address, A] =
    state.collectAddressLposPortfolios(pf)

  override def append(
      diff: Diff,
      carryFee: Long,
      block: Block,
      consensusPostActionDiff: ConsensusPostActionDiff,
      certificates: Set[X509Certificate]
  ): Unit = state.append(diff, carryFee, block, consensusPostActionDiff, certificates)

  override def rollbackTo(targetBlockId: ByteStr): Either[String, Seq[Block]] = state.rollbackTo(targetBlockId)

  override def permissions(acc: Address): Permissions = state.permissions(acc)

  override def miners: MinerQueue = state.miners

  override def contractValidators: ContractValidatorPool = state.contractValidators

  override def lastBlockContractValidators: Set[Address] = state.lastBlockContractValidators

  override def minerBanHistory(address: Address): MinerBanHistory = state.minerBanHistory(address)

  override def bannedMiners(height: Int): Seq[Address] = state.bannedMiners(height)

  override def warningsAndBans: Map[Address, Seq[MinerBanlistEntry]] = state.warningsAndBans

  override def participantPubKey(address: Address): Option[PublicKeyAccount] = state.participantPubKey(address)

  override def networkParticipants(): Seq[Address] = state.networkParticipants()

  override def policies(): Set[ByteStr] = state.policies()

  override def policyExists(policyId: ByteStr): Boolean = state.policyExists(policyId)

  override def policyOwners(policyId: ByteStr): Set[Address] = state.policyOwners(policyId)

  override def policyRecipients(policyId: ByteStr): Set[Address] = state.policyRecipients(policyId)

  override def policyDataHashes(policyId: ByteStr): Set[PolicyDataHash] = state.policyDataHashes(policyId)

  override def policyDataHashExists(policyId: ByteStr, dataHash: PolicyDataHash): Boolean = state.policyDataHashExists(policyId, dataHash)

  override def allNonEmptyRoleAddresses: Stream[Address] = state.allNonEmptyRoleAddresses

  override def contracts(): Set[ContractInfo] = state.contracts()

  override def contract(contractId: ByteStr): Option[ContractInfo] = state.contract(contractId)

  override def contractKeys(request: KeysRequest, readingContext: ContractReadingContext): Vector[String] =
    state.contractKeys(request, readingContext)

  override def contractData(contractId: ByteStr, readingContext: ContractReadingContext): ExecutedContractData =
    state.contractData(contractId, readingContext)

  override def contractData(contractId: ByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] =
    state.contractData(contractId, key, readingContext)

  override def contractData(contractId: ByteStr, keys: Iterable[String], readingContext: ContractReadingContext): ExecutedContractData =
    state.contractData(contractId, keys, readingContext)

  override def executedTxFor(forTxId: ByteStr): Option[ExecutedContractTransaction] = state.executedTxFor(forTxId)

  override def hasExecutedTxFor(forTxId: ByteStr): Boolean = state.hasExecutedTxFor(forTxId)

  override def policyDataHashTxId(id: PolicyDataId): Option[ByteStr] = state.policyDataHashTxId(id)

  override def certByDistinguishedNameHash(dnHash: String): Option[Certificate] = state.certByDistinguishedNameHash(dnHash)

  override def certByDistinguishedName(distinguishedName: String): Option[Certificate] = state.certByDistinguishedName(distinguishedName)

  override def certByPublicKey(publicKey: PublicKeyAccount): Option[Certificate] = state.certByPublicKey(publicKey)

  override def certByFingerPrint(fingerprint: ByteStr): Option[Certificate] = state.certByFingerPrint(fingerprint)

  override def certsAtHeight(height: Int): Set[Certificate] = state.certsAtHeight(height)

  override def aliasesIssuedByAddress(address: Address): Set[Alias] = state.aliasesIssuedByAddress(address)
}
