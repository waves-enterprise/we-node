package com.wavesenterprise.utils

import cats.kernel.Monoid
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
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.transaction.Transaction.Type
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.docker.{ExecutedContractData, ExecutedContractTransaction}
import com.wavesenterprise.transaction.lease.LeaseTransaction
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.{AssetId, Transaction, ValidationError}

object EmptyBlockchain extends Blockchain {
  override def height: Int = 0

  override def score: BigInt = 0

  override def scoreOf(blockId: ByteStr): Option[BigInt] = None

  override def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = None

  override def blockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)] = None

  override def lastBlock: Option[Block] = None

  override def lastPersistenceBlock: Option[Block] = None

  override def carryFee: Long = 0

  override def blockBytes(height: Int): Option[Array[Byte]] = None

  override def blockBytes(blockId: ByteStr): Option[Array[Byte]] = None

  override def heightOf(blockId: ByteStr): Option[Int] = None

  /** Returns the most recent block IDs, starting from the most recent  one */
  override def lastBlockIds(howMany: Int): Seq[ByteStr] = Seq.empty

  override def lastBlockIds(startBlock: BlockId, howMany: Int): Option[Seq[BlockId]] = None

  /** Returns a chain of blocks starting with the block with the given ID (from oldest to newest) */
  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int): Option[Seq[ByteStr]] = None

  override def parent(block: Block, back: Int): Option[Block] = None

  override def parentHeader(block: Block): Option[BlockHeader] = None

  /** Features related */
  override def approvedFeatures: Map[Short, Int] = Map.empty

  override def activatedFeatures: Map[Short, Int] = Map.empty

  override def featureVotes(height: Int): Map[Short, Int] = Map.empty

  override def portfolio(a: Address): Portfolio = Portfolio.empty

  override def transactionInfo(id: ByteStr): Option[(Int, Transaction)] = None

  override def transactionHeight(id: ByteStr): Option[Int] = None

  override def addressTransactions(address: Address, types: Set[Type], count: Int, fromId: Option[ByteStr]): Either[String, Seq[(Int, Transaction)]] =
    Right(Seq.empty)

  override def containsTransaction(tx: Transaction): Boolean = false

  override def containsTransaction(id: ByteStr): Boolean = false

  override def assets(): Set[AssetId] = Set.empty

  override def asset(id: AssetId): Option[AssetInfo] = None

  override def assetDescription(id: ByteStr): Option[AssetDescription] = None

  override def resolveAlias(a: Alias): Either[ValidationError, Address] = Left(GenericError("Empty blockchain"))

  override def leaseDetails(leaseId: ByteStr): Option[LeaseDetails] = None

  override def filledVolumeAndFee(orderId: ByteStr): VolumeAndFee = VolumeAndFee(0, 0)

  /** Retrieves Waves balance snapshot in the [from, to] range (inclusive) */
  override def balanceSnapshots(address: Address, from: Int, to: Int): Seq[BalanceSnapshot] = Seq.empty

  override def accounts(): Set[Address] = Set.empty

  override def accountScript(address: Address): Option[Script] = None

  override def hasScript(address: Address): Boolean = false

  override def assetScript(asset: AssetId): Option[Script] = None

  override def hasAssetScript(asset: AssetId): Boolean = false

  def accountDataSlice(acc: Address, from: Int, to: Int): AccountDataInfo = AccountDataInfo(Map.empty)

  override def accountData(acc: Address): AccountDataInfo = AccountDataInfo(Map.empty)

  override def accountData(acc: Address, key: String): Option[DataEntry[_]] = None

  override def balance(address: Address, mayBeAssetId: Option[AssetId]): Long = 0

  override def leaseBalance(address: Address): LeaseBalance = LeaseBalance.empty

  override def assetDistribution(assetId: ByteStr): AssetDistribution = Monoid.empty[AssetDistribution]

  override def westDistribution(height: Int): Map[Address, Long] = Map.empty

  override def allActiveLeases: Set[LeaseTransaction] = Set.empty

  override def assetDistributionAtHeight(assetId: AssetId,
                                         height: Int,
                                         count: Int,
                                         fromAddress: Option[Address]): Either[ValidationError, AssetDistributionPage] =
    Right(AssetDistributionPage(Paged[Address, AssetDistribution](false, None, Monoid.empty[AssetDistribution])))

  /** Builds a new portfolio map by applying a partial function to all portfolios on which the function is defined.
    *
    * @note Portfolios passed to `pf` only contain Waves and Leasing balances to improve performance */
  override def collectLposPortfolios[A](pf: PartialFunction[(Address, Portfolio), A]): Map[Address, A]                  = Map.empty
  override def append(diff: Diff, carryFee: Long, block: Block, consensusPostActionDiff: ConsensusPostActionDiff): Unit = ()
  override def rollbackTo(targetBlockId: ByteStr): Either[String, Seq[Block]]                                           = Right(Seq.empty)

  override def permissions(acc: Address): Permissions = Permissions.empty

  override def miners: MinerQueue = MinerQueue.empty

  override def contractValidators: ContractValidatorPool = ContractValidatorPool.empty

  override def lastBlockContractValidators: Set[Address] = Set.empty

  override def minerBanHistory(address: Address): MinerBanHistory = {
    new MinerBanHistoryBuilderV2(0, Int.MaxValue).empty
  }

  override def bannedMiners(height: Int): Seq[Address] = Seq.empty

  override def warningsAndBans: Map[Address, Seq[MinerBanlistEntry]] = Map.empty

  override def participantPubKey(address: Address): Option[PublicKeyAccount] = None

  override def networkParticipants(): Seq[Address] = Seq.empty

  override def policies(): Set[AssetId] = Set.empty

  override def policyExists(policyId: ByteStr): Boolean = false

  override def policyOwners(policyId: ByteStr): Set[Address] = Set.empty

  override def policyRecipients(policyId: ByteStr): Set[Address] = Set.empty

  override def contracts(): Set[ContractInfo] = Set.empty

  override def contract(contractId: ByteStr): Option[ContractInfo] = None

  override def contractKeys(keysRequest: KeysRequest, readingContext: ContractReadingContext): Vector[String] = Vector.empty

  override def contractData(contractId: ByteStr, keys: Iterable[String], readingContext: ContractReadingContext): ExecutedContractData =
    ExecutedContractData(Map.empty)

  override def contractData(contractId: ByteStr, readingContext: ContractReadingContext): ExecutedContractData = ExecutedContractData(Map.empty)

  override def contractData(contractId: ByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] = None

  override def executedTxFor(forTxId: ByteStr): Option[ExecutedContractTransaction] = None

  override def hasExecutedTxFor(forTxId: ByteStr): Boolean = false

  override def policyDataHashes(policyId: ByteStr): Set[PolicyDataHash] = Set.empty

  override def policyDataHashExists(policyId: ByteStr, dataHash: PolicyDataHash): Boolean = false

  override def policyDataHashTxId(id: PolicyDataId): Option[AssetId] = None

  override def allNonEmptyRoleAddresses: Stream[Address] = Stream.empty
}
