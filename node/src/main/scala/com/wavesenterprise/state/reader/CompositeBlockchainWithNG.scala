package com.wavesenterprise.state.reader

import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, BlockHeader}
import com.wavesenterprise.consensus.ContractValidatorsProvider
import com.wavesenterprise.state.{Blockchain, ByteStr, Diff, NG}
import com.wavesenterprise.transaction.Transaction

/**
  * [[CompositeBlockchain]] descendant with actual NG and mining information.
  */
class CompositeBlockchainWithNG private (ng: NG, inner: Blockchain, diff: Diff, contractValidatorsProvider: ContractValidatorsProvider)
    extends CompositeBlockchain(inner, Some(diff), contractValidatorsProvider = contractValidatorsProvider) {

  override def height: Int = ng.height

  override def score: BigInt = ng.score

  override def scoreOf(blockId: ByteStr): Option[BigInt] = ng.scoreOf(blockId)

  override def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = ng.blockHeaderAndSize(height)

  override def blockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)] = ng.blockHeaderAndSize(blockId)

  override def lastBlock: Option[Block] = ng.lastBlock

  override def lastPersistenceBlock: Option[Block] = inner.lastPersistenceBlock

  override def carryFee: Long = ng.carryFee

  override def blockBytes(height: Int): Option[Array[Transaction.Type]] = ng.blockBytes(height)

  override def blockBytes(blockId: ByteStr): Option[Array[Transaction.Type]] = ng.blockBytes(blockId)

  override def heightOf(blockId: ByteStr): Option[Int] = ng.heightOf(blockId)

  override def lastBlockIds(howMany: Int): Seq[ByteStr] = ng.lastBlockIds(howMany)

  override def lastBlockIds(startBlock: BlockId, howMany: Int): Option[Seq[BlockId]] = ng.lastBlockIds(startBlock, howMany)

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int): Option[Seq[ByteStr]] = ng.blockIdsAfter(parentSignature, howMany)

  override def parent(block: Block, back: Int): Option[Block] = ng.parent(block, back)

  override def parentHeader(block: Block): Option[BlockHeader] = ng.parentHeader(block)

  override def approvedFeatures: Map[Short, Int] = ng.approvedFeatures

  override def activatedFeatures: Map[Short, Int] = ng.activatedFeatures

  override def featureVotes(height: Int): Map[Short, Int] = ng.featureVotes(height)
}

object CompositeBlockchainWithNG {

  def apply(ng: NG, inner: Blockchain, diff: Diff): CompositeBlockchainWithNG = {
    new CompositeBlockchainWithNG(ng, inner, diff, new ContractValidatorsProvider(inner))
  }
}
