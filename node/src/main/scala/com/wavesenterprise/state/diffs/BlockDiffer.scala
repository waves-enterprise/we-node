package com.wavesenterprise.state.diffs

import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.block.{Block, MicroBlock}
import com.wavesenterprise.database.snapshot.ConsensualSnapshotSettings
import com.wavesenterprise.mining.{MiningConstraint, MultiDimensionalMiningConstraint}
import com.wavesenterprise.settings.BlockchainSettings
import com.wavesenterprise.state.diffs.BlockDifferBase.{BlockDiffResult, MicroBlockDiffResult}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.ValidationError

import scala.concurrent.duration.FiniteDuration

object BlockDiffer extends BlockDifferBase {

  def fromBlock(blockchainSettings: BlockchainSettings,
                snapshotSettings: ConsensualSnapshotSettings,
                blockchain: Blockchain,
                permissionValidator: PermissionValidator,
                maybePrevBlock: Option[Block],
                block: Block,
                constraint: MiningConstraint,
                txExpireTimeout: FiniteDuration,
                alreadyVerified: Boolean = false,
                alreadyVerifiedTxIds: Set[ByteStr] = Set.empty): Either[ValidationError, BlockDiffResult] = {
    val txDiffer = TransactionDiffer(
      settings = blockchainSettings,
      permissionValidator = permissionValidator,
      prevBlockTimestamp = maybePrevBlock.map(_.timestamp),
      currentBlockTimestamp = block.timestamp,
      currentBlockHeight = blockchain.height + 1,
      txExpireTimeout = txExpireTimeout,
      blockOpt = Some(block),
      minerOpt = Some(block.sender),
      alreadyVerified = alreadyVerified,
      alreadyVerifiedTxIds = alreadyVerifiedTxIds
    )

    super.diffFromBlock(
      snapshotSettings = snapshotSettings,
      blockchain = blockchain,
      maybePrevBlock = maybePrevBlock,
      block = block,
      constraint = constraint,
      transactionDiffer = txDiffer,
      alreadyVerified = alreadyVerified
    )
  }

  def fromMicroBlock(blockchainSettings: BlockchainSettings,
                     snapshotSettings: ConsensualSnapshotSettings,
                     blockchain: Blockchain,
                     permissionValidator: PermissionValidator,
                     prevBlockTimestamp: Option[Long],
                     micro: MicroBlock,
                     timestamp: Long,
                     constraint: MultiDimensionalMiningConstraint,
                     txExpireTimeout: FiniteDuration,
                     alreadyVerified: Boolean = false,
                     alreadyVerifiedTxIds: Set[ByteStr] = Set.empty): Either[ValidationError, MicroBlockDiffResult] = {
    val txDiffer = TransactionDiffer(
      settings = blockchainSettings,
      permissionValidator = permissionValidator,
      prevBlockTimestamp = prevBlockTimestamp,
      currentBlockTimestamp = timestamp,
      currentBlockHeight = blockchain.height,
      txExpireTimeout = txExpireTimeout,
      blockOpt = Some(micro),
      alreadyVerified = alreadyVerified,
      alreadyVerifiedTxIds = alreadyVerifiedTxIds
    )

    super.diffFromMicroBlock(
      snapshotSettings = snapshotSettings,
      blockchain = blockchain,
      micro = micro,
      constraint = constraint,
      transactionDiffer = txDiffer
    )
  }
}
