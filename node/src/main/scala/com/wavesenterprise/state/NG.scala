package com.wavesenterprise.state

import com.wavesenterprise.account.Address
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, BlockHeader, MicroBlock}
import com.wavesenterprise.certs.CertChainStore

trait NG extends Blockchain {
  def microBlock(id: BlockId): Option[MicroBlock]

  def bestLastBlockInfo(maxTimestamp: Long): Option[BlockMinerInfo]

  def lastPersistedBlockIds(count: Int): Seq[BlockId]

  def lastMicroBlock: Option[MicroBlock]

  def currentBaseBlock: Option[Block]

  def currentMiner: Option[Address]

  def microblockIds: Seq[BlockId]

  def blockHeaderByIdWithLiquidVariations(blockId: BlockId): Option[BlockHeader]

  def liquidBlockById(blockId: BlockId): Option[Block]

  def certChainStoreByBlockId(blockId: BlockId): Option[CertChainStore]

  def crlHashesByBlockId(blockId: BlockId): Set[ByteStr]
}
