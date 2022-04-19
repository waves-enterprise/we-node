package com.wavesenterprise.state

import com.wavesenterprise.account.Address
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, BlockHeader, MicroBlock}

trait NG extends Blockchain {
  def microBlock(id: ByteStr): Option[MicroBlock]

  def bestLastBlockInfo(maxTimestamp: Long): Option[BlockMinerInfo]

  def lastPersistedBlockIds(count: Int): Seq[BlockId]

  def lastMicroBlock: Option[MicroBlock]

  def currentBaseBlock: Option[Block]

  def currentMiner: Option[Address]

  def microblockIds: Seq[BlockId]

  def blockHeaderByIdWithLiquidVariations(blockId: ByteStr): Option[BlockHeader]

  def liquidBlockById(blockId: ByteStr): Option[Block]
}
