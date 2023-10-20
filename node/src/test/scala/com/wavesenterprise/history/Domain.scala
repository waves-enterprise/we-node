package com.wavesenterprise.history

import com.wavesenterprise.account.Address
import com.wavesenterprise.block.{Block, DiscardedBlocks}
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction._

case class Domain(blockchainUpdater: BlockchainUpdater with NG, storage: MainRocksDBStorage) {
  def effBalance(a: Address): Long =
    blockchainUpdater.effectiveBalance(a, blockchainUpdater.height, 1000)

  def appendBlock(b: Block, postAction: ConsensusPostAction = ConsensusPostAction.NoAction): Option[DiscardedTransactions] =
    blockchainUpdater.processBlock(b, postAction).explicitGet()

  def tryAppendBlock(b: Block,
                     postAction: ConsensusPostAction = ConsensusPostAction.NoAction): Either[ValidationError, Option[DiscardedTransactions]] =
    blockchainUpdater.processBlock(b, postAction)

  def removeAfter(blockId: ByteStr): DiscardedBlocks =
    blockchainUpdater.removeAfter(blockId).explicitGet()

  def lastBlockId: ByteStr = blockchainUpdater.lastBlockId.get

  def portfolio(address: Address): Portfolio =
    blockchainUpdater.addressPortfolio(address)

  def addressTransactions(address: Address): Either[String, Seq[(Int, Transaction)]] =
    blockchainUpdater.addressTransactions(address, Set.empty, 128, None)

  def carryFee: Long =
    blockchainUpdater.carryFee
}
