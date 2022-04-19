package com.wavesenterprise.transaction

import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, DiscardedBlocks, MicroBlock}
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.database.PrivacyLostItemUpdater
import com.wavesenterprise.privacy.PolicyDataId
import com.wavesenterprise.state.appender.BaseAppender.BlockType
import com.wavesenterprise.state.appender.BaseAppender.BlockType.Hard
import com.wavesenterprise.state.{BlockchainEvent, ByteStr, NgState}
import monix.reactive.Observable

trait BlockchainUpdater extends PrivacyLostItemUpdater {
  val permissionValidator: PermissionValidator

  def processBlock(block: Block,
                   postAction: ConsensusPostAction,
                   blockType: BlockType = Hard,
                   isOwn: Boolean = false,
                   alreadyVerifiedTxIds: Set[ByteStr] = Set.empty): Either[ValidationError, Option[DiscardedTransactions]]

  def processMicroBlock(microBlock: MicroBlock, isOwn: Boolean = false, alreadyVerifiedTxIds: Set[ByteStr] = Set.empty): Either[ValidationError, Unit]

  def removeAfter(blockId: ByteStr): Either[ValidationError, DiscardedBlocks]

  def ngState: Option[NgState]

  def lastBlockInfo: Observable[LastBlockInfo]

  def lastBlockchainEvent: Observable[BlockchainEvent]

  def policyUpdates: Observable[PolicyUpdate]

  def policyRollbacks: Observable[PolicyDataId]

  def isLastBlockId(id: ByteStr): Boolean

  def isRecentlyApplied(id: ByteStr): Boolean

  def shutdown(): Unit
}

case class LastBlockInfo(id: BlockId, height: Int, score: BigInt, ready: Boolean)

case class PolicyUpdate(key: PolicyDataId, maybeTxTs: Option[Long])
