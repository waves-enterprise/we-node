package com.wavesenterprise.state

import cats.kernel.Monoid
import com.google.common.cache.{Cache, CacheBuilder}
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, DiscardedMicroBlocks, MicroBlock, TxMicroBlock, VoteMicroBlock}
import com.wavesenterprise.consensus.{CftLikeConsensusBlockData, ConsensusPostActionDiff, Vote}
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.state.appender.BaseAppender.BlockType
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.ScorexLogging

import java.util.concurrent.TimeUnit
import scala.collection.mutable.{ListBuffer => MList, Map => MMap}

/* This is not thread safe, used only from BlockchainUpdaterImpl */
class NgState(val base: Block,
              val baseBlockType: BlockType,
              val baseBlockDiff: Diff,
              val baseBlockCarry: Long,
              val baseBlockTotalFee: Long,
              val approvedFeatures: Set[Short],
              val consensusPostActionDiff: ConsensusPostActionDiff)
    extends ScorexLogging {

  private[this] case class CachedMicroDiff(diff: Diff, carryFee: Long, totalFee: Long, timestamp: Long)

  private[this] val MaxTotalDiffs = 15

  private[this] val microDiffs: MMap[BlockId, CachedMicroDiff]         = MMap.empty
  private[this] val microBlocks: MList[MicroBlock]                     = MList.empty // fresh head
  private[this] val certStoresByBlockId: MMap[BlockId, CertChainStore] = MMap.empty // TODO: validate everything
  private[this] val crlHashesByBlockId: MMap[BlockId, Set[ByteStr]]    = MMap.empty

  def microBlockIds: Seq[BlockId] =
    microBlocks.map(_.totalLiquidBlockSig)

  def diffFor(totalResBlockSig: BlockId): (Diff, Long, Long) =
    if (totalResBlockSig == base.uniqueId)
      (baseBlockDiff, baseBlockCarry, baseBlockTotalFee)
    else
      internalCaches.blockDiffCache.get(
        totalResBlockSig, { () =>
          microBlocks.find(_.totalLiquidBlockSig == totalResBlockSig) match {
            case Some(current) =>
              val (prevDiff, prevCarry, prevTotalFee)                   = this.diffFor(current.prevLiquidBlockSig)
              val CachedMicroDiff(currDiff, currCarry, currTotalFee, _) = this.microDiffs(totalResBlockSig)
              (Monoid.combine(prevDiff, currDiff), prevCarry + currCarry, prevTotalFee + currTotalFee)

            case None =>
              (Diff.empty, 0L, 0L)
          }
        }
      )

  def bestLiquidBlockId: BlockId =
    microBlocks.headOption.map(_.totalLiquidBlockSig).getOrElse(base.uniqueId)

  def lastMicroBlock: Option[MicroBlock] =
    microBlocks.headOption

  def transactions: Seq[Transaction] =
    base.transactionData.toVector ++ microBlocks.view
      .map {
        case txMicro: TxMicroBlock => txMicro.transactionData
        case _: VoteMicroBlock     => Seq.empty
      }
      .reverse
      .flatten

  def votes: Seq[Vote] =
    microBlocks
      .collectFirst {
        case voteMicro: VoteMicroBlock => voteMicro.votes
      }
      .getOrElse(Nil)

  def bestLiquidBlock: Block =
    if (microBlocks.isEmpty)
      base
    else
      internalCaches.bestBlockCache match {
        case Some(cachedBlock) =>
          cachedBlock

        case None =>
          val signerData = base.blockHeader.signerData.copy(signature = microBlocks.head.totalLiquidBlockSig)
          val consensusBlockData = base.consensusData match {
            case cftData: CftLikeConsensusBlockData =>
              cftData.copy(votes = votes)
            case other => other
          }
          val block =
            Block.build(base.version, base.timestamp, base.reference, consensusBlockData, transactions, signerData, base.featureVotes).explicitGet()
          internalCaches.bestBlockCache = Some(block)
          block
      }

  def totalDiffOf(id: BlockId): Option[(Block, Diff, Long, Long, DiscardedMicroBlocks)] =
    forgeBlock(id).map {
      case (block, discarded) =>
        val (diff, carry, totalFee) = this.diffFor(id)
        (block, diff, carry, totalFee, discarded)
    }

  def certStoreForBlock(id: BlockId): Option[CertChainStore] = certStoresByBlockId.get(id)

  def crlHashesForBlock(id: BlockId): Set[ByteStr] = crlHashesByBlockId.get(id).toSet.flatten

  def bestLiquidDiffAndFees: (Diff, Long, Long) =
    microBlocks.headOption.fold((baseBlockDiff, baseBlockCarry, baseBlockTotalFee))(m => diffFor(m.totalLiquidBlockSig))

  def bestLiquidDiff: Diff =
    bestLiquidDiffAndFees._1

  def contains(blockId: BlockId): Boolean = base.uniqueId == blockId || microDiffs.contains(blockId)

  def microBlock(id: BlockId): Option[MicroBlock] = microBlocks.find(_.totalLiquidBlockSig == id)

  def bestLastBlockInfo(maxTimeStamp: Long): BlockMinerInfo = {
    val blockId = microBlocks
      .find(micro => microDiffs(micro.totalLiquidBlockSig).timestamp <= maxTimeStamp)
      .map(_.totalLiquidBlockSig)
      .getOrElse(base.uniqueId)
    BlockMinerInfo(base.consensusData, base.timestamp, blockId)
  }

  def append(m: MicroBlock,
             diff: Diff,
             microblockCarry: Long,
             microblockTotalFee: Long,
             certChainStore: CertChainStore,
             crlHashes: Set[ByteStr]): Unit = {
    microDiffs.put(m.totalLiquidBlockSig, CachedMicroDiff(diff, microblockCarry, microblockTotalFee, m.timestamp))
    microBlocks.prepend(m)
    if (CertChainStore.empty != certChainStore) {
      certStoresByBlockId.put(m.totalLiquidBlockSig, certChainStore)
    }
    if (crlHashes.nonEmpty) {
      crlHashesByBlockId.put(m.totalLiquidBlockSig, crlHashes)
    }
    internalCaches.invalidate(m.totalLiquidBlockSig)
  }

  def carryFee: Long =
    baseBlockCarry + microDiffs.values.map(_.carryFee).sum

  def liquidBlock(blockId: BlockId): Option[Block] = forgeBlock(blockId).map { case (block, _) => block }

  private[this] def forgeBlock(blockId: BlockId): Option[(Block, DiscardedMicroBlocks)] =
    internalCaches.forgedBlockCache.get(
      blockId, { () =>
        val microBlocksAsc = microBlocks.reverse

        if (base.uniqueId == blockId) {
          Some(base -> microBlocksAsc)
        } else if (!microBlocksAsc.exists(_.totalLiquidBlockSig == blockId)) {
          None
        } else {
          val (accumulatedTxs, maybeFound) = microBlocksAsc.foldLeft((Vector.empty[Transaction], Option.empty[(ByteStr, DiscardedMicroBlocks)])) {
            case ((txsAcc, Some((sig, discarded))), micro) =>
              txsAcc -> Some((sig, micro +: discarded))

            case ((txsAcc, None), micro) =>
              val discarded = Some(micro.totalLiquidBlockSig -> Seq.empty[MicroBlock])
                .filter { case (id, _) => id == blockId }

              val transactionData = micro match {
                case txMicro: TxMicroBlock => txMicro.transactionData
                case _: VoteMicroBlock     => Seq.empty
              }

              (txsAcc ++ transactionData, discarded)
          }

          maybeFound.map {
            case (sig, discarded) =>
              val signerData = base.blockHeader.signerData.copy(signature = sig)
              val consensusBlockData = base.consensusData match {
                case cftData: CftLikeConsensusBlockData =>
                  cftData.copy(votes = votes)
                case other => other
              }
              val block = Block
                .build(
                  version = base.version,
                  timestamp = base.timestamp,
                  reference = base.reference,
                  consensusData = consensusBlockData,
                  transactionData = base.transactionData ++ accumulatedTxs,
                  signerData = signerData,
                  featureVotes = base.featureVotes
                )
                .explicitGet()

              block -> discarded
          }
        }
      }
    )

  private[this] object internalCaches {

    val blockDiffCache: Cache[BlockId, (Diff, Long, Long)] = CacheBuilder
      .newBuilder()
      .maximumSize(MaxTotalDiffs)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build[BlockId, (Diff, Long, Long)]()

    val forgedBlockCache: Cache[BlockId, Option[(Block, DiscardedMicroBlocks)]] = CacheBuilder
      .newBuilder()
      .maximumSize(MaxTotalDiffs)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build[BlockId, Option[(Block, DiscardedMicroBlocks)]]()

    @volatile
    var bestBlockCache = Option.empty[Block]

    def invalidate(newBlockId: BlockId): Unit = {
      forgedBlockCache.invalidateAll()
      blockDiffCache.invalidate(newBlockId)
      bestBlockCache = None
    }
  }
}
