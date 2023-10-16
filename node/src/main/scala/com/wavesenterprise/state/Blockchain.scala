package com.wavesenterprise.state

import com.wavesenterprise.account.{Address, Alias, PublicKeyAccount}
import com.wavesenterprise.acl.Permissions
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, BlockHeader}
import com.wavesenterprise.consensus._
import com.wavesenterprise.database.RollbackResult
import com.wavesenterprise.database.certs.CertificatesState
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.{AssetId, Transaction, ValidationError}

import java.security.cert.X509Certificate

trait Blockchain extends ContractBlockchain with PrivacyBlockchain with CertificatesState {
  def height: Int
  def score: BigInt
  def scoreOf(blockId: ByteStr): Option[BigInt]

  def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)]
  def blockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)]

  def lastBlock: Option[Block]
  def lastPersistenceBlock: Option[Block]
  def carryFee: Long
  def blockBytes(height: Int): Option[Array[Byte]]
  def blockBytes(blockId: ByteStr): Option[Array[Byte]]

  def heightOf(blockId: ByteStr): Option[Int]

  /** Returns the most recent block IDs, starting from the most recent one */
  def lastBlockIds(howMany: Int): Seq[ByteStr]

  /** Returns the most recent block IDs, starting from the startBlock */
  def lastBlockIds(startBlock: BlockId, howMany: Int): Option[Seq[BlockId]]

  /** Returns a chain of blocks starting with the block with the given ID (from oldest to newest) */
  def blockIdsAfter(parentSignature: ByteStr, howMany: Int): Option[Seq[ByteStr]]

  def parent(block: Block, back: Int = 1): Option[Block]

  def parentHeader(block: Block): Option[BlockHeader]

  /** Features related */
  def approvedFeatures: Map[Short, Int]
  def activatedFeatures: Map[Short, Int]
  def featureVotes(height: Int): Map[Short, Int]

  def addressPortfolio(a: Address): Portfolio
  def assetHolderPortfolio(assetHolder: AssetHolder): Portfolio =
    assetHolder.product(addressPortfolio, contractPortfolio)

  def transactionInfo(id: ByteStr): Option[(Int, Transaction)]
  def transactionHeight(id: ByteStr): Option[Int]

  def addressTransactions(address: Address,
                          txTypes: Set[Transaction.Type],
                          count: Int,
                          fromId: Option[ByteStr]): Either[String, Seq[(Int, Transaction)]]

  def containsTransaction(tx: Transaction): Boolean
  def containsTransaction(id: ByteStr): Boolean

  def assets(): Set[ByteStr]
  def asset(id: ByteStr): Option[AssetInfo]
  def assetDescription(id: ByteStr): Option[AssetDescription]

  def resolveAlias(a: Alias): Either[ValidationError, Address]

  def leaseDetails(leaseId: LeaseId): Option[LeaseDetails]

  def filledVolumeAndFee(orderId: ByteStr): VolumeAndFee

  /** Retrieves WEST balance snapshot in the [from, to] range (inclusive) */
  def addressBalanceSnapshots(address: Address, from: Int, to: Int): Seq[BalanceSnapshot]

  def accounts(): Set[Address]
  def accountScript(address: Address): Option[Script]
  def hasScript(address: Address): Boolean

  def assetScript(id: ByteStr): Option[Script]
  def hasAssetScript(id: ByteStr): Boolean

  def accountDataSlice(acc: Address, from: Int, to: Int): AccountDataInfo
  def accountData(acc: Address): AccountDataInfo
  def accountData(acc: Address, key: String): Option[DataEntry[_]]

  def addressLeaseBalance(address: Address): LeaseBalance

  def assetHolderBalance(assetHolder: AssetHolder, mayBeAssetId: Option[AssetId] = None): Long =
    assetHolder.product(addressBalance(_, mayBeAssetId), contractBalance(_, mayBeAssetId, ContractReadingContext.Default))

  def addressBalance(address: Address, mayBeAssetId: Option[AssetId] = None): Long

  def addressAssetDistribution(assetId: ByteStr): AssetDistribution

  def addressAssetDistributionAtHeight(assetId: AssetId,
                                       height: Int,
                                       count: Int,
                                       fromAddress: Option[Address]): Either[ValidationError, AssetDistributionPage]

  def addressWestDistribution(height: Int): Map[Address, Long]

  /** Builds a new portfolio map by applying a partial function to all portfolios on which the function is defined.
    *
    * @note Portfolios passed to `pf` only contain WEST and Leasing balances to improve performance */
  def collectAddressLposPortfolios[A](pf: PartialFunction[(Address, Portfolio), A]): Map[Address, A]

  def append(
      diff: Diff,
      carryFee: Long,
      block: Block,
      consensusPostActionDiff: ConsensusPostActionDiff = ConsensusPostActionDiff.empty,
      certificates: Set[X509Certificate] = Set.empty
  ): Int

  def rollbackTo(targetBlockId: ByteStr): Either[String, RollbackResult]

  def permissions(acc: Address): Permissions

  def miners: MinerQueue

  def contractValidators: ContractValidatorPool

  def lastBlockContractValidators: Set[Address]

  def minerBanHistory(address: Address): MinerBanHistory

  def bannedMiners(height: Int): Seq[Address]

  def warningsAndBans: Map[Address, Seq[MinerBanlistEntry]]

  def participantPubKey(address: Address): Option[PublicKeyAccount]

  def networkParticipants(): Seq[Address]

  def allNonEmptyRoleAddresses: Stream[Address]

  def aliasesIssuedByAddress(address: Address): Set[Alias]
}
