package com.wavesenterprise.state.appender

import cats.implicits._
import com.wavesenterprise.block.{Block, BlockIdsCache, DiscardedBlocks}
import com.wavesenterprise.consensus.Consensus
import com.wavesenterprise.metrics.Instrumented
import com.wavesenterprise.network.MicroBlockLoader
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.SynchronizationSettings.KeyBlockAppendingSettings
import com.wavesenterprise.state.appender.BaseAppender.BlockType
import com.wavesenterprise.state.appender.BaseAppender.BlockType.{Hard, Liquid}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.BlockchainEventError.BlockAppendError
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.docker.ExecutedContractTransaction
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.utx.UtxPool
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.util.{Left, Right}

/**
  * Provides functionality for validating and adding blocks to blockchain.
  */
class BaseAppender(
    blockchainUpdater: BlockchainUpdater with Blockchain,
    utxStorage: UtxPool,
    consensus: Consensus,
    time: Time,
    microBlockLoader: MicroBlockLoader,
    keyBlockAppendingSettings: KeyBlockAppendingSettings,
    keyBlockIdsCache: BlockIdsCache,
    activePeerConnections: ActivePeerConnections
)(implicit scheduler: Scheduler)
    extends ScorexLogging
    with Instrumented {

  private val blockProcessingTimeStats = Kamon.histogram("single-block-processing-time")

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSS")

  private def formatBlockTime(millis: Long): String = {
    val instant          = Instant.ofEpochMilli(millis)
    val zonedDateTimeUtc = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
    dateTimeFormatter.format(zonedDateTimeUtc)
  }

  def processBroadcastKeyBlock(
      keyBlock: Block,
      alreadyVerifiedTxIds: Set[ByteStr] = Set.empty,
      maxAttempts: Int = keyBlockAppendingSettings.maxAttempts.value
  ): Task[Either[ValidationError, Option[BigInt]]] = {
    def measuredAction: Either[ValidationError, Option[BigInt]] = {
      blockchainUpdater.lastBlock
        .map { lastBlock =>
          if (lastBlock.uniqueId == keyBlock.reference) {
            appendBlock(keyBlock, blockType = Liquid, alreadyVerifiedTxIds = alreadyVerifiedTxIds, certChainStore = CertChainStore.empty)
              .map(_ => Some(blockchainUpdater.score))
          } else if (blockchainUpdater.contains(keyBlock.uniqueId)) {
            Right(None)
          } else if (consensus.blockCanBeReplaced(time.correctedTime(), keyBlock, lastBlock)) {
            replaceNotFinalizedBlock(keyBlock, lastBlock)
          } else {
            val lastBlockTime = formatBlockTime(lastBlock.timestamp)
            val newBlockTime  = formatBlockTime(keyBlock.timestamp)

            Left(
              BlockAppendError(
                s"Broadcast block is not a child of the last block. Last block '$lastBlock', lastBlockTime: '$lastBlockTime', newBlockTime: '$newBlockTime'",
                keyBlock
              ))
          }
        }
        .getOrElse {
          Left(BlockAppendError(s"Last block not found", keyBlock))
        }
    }

    Task {
      measureSuccessful(blockProcessingTimeStats, measuredAction)
    }.flatMap {
        case Left(BlockAppendError(_, _)) if microBlockLoader.storage.isKnownMicroBlock(keyBlock.reference) && maxAttempts > 1 =>
          Task
            .defer {
              log.debug("Broadcast block is not a child of the last block, but block refers to a known micro-block, retrying")
              processBroadcastKeyBlock(keyBlock, alreadyVerifiedTxIds, maxAttempts - 1)
            }
            .delayExecution(keyBlockAppendingSettings.retryInterval)
        case result =>
          Task(keyBlockIdsCache.put(keyBlock.uniqueId)).as(result)
      }
      .executeOn(scheduler)
  }

  private def replaceNotFinalizedBlock(newBlock: Block, lastBlock: Block): Either[ValidationError, Option[BigInt]] = {
    log.warn(s"Last block '$lastBlock' not finalized. Replace it with broadcasted key block '$newBlock'")
    blockchainUpdater.removeAfter(lastBlock.reference).flatMap { droppedBlocks =>
      appendBlock(newBlock, Liquid, certChainStore = CertChainStore.empty)
        .leftFlatMap { err =>
          rollbackDroppedBlocks(lastBlock.reference, droppedBlocks) >> Left(err)
        }
        .map { _ =>
          sendTransactionsBackToUtx(droppedBlocks)
          Some(blockchainUpdater.score)
        }
    }
  }

  def processMinedKeyBlock(keyBlock: Block): Task[Either[ValidationError, BigInt]] =
    for {
      result <- Task {
        measureSuccessful(
          blockProcessingTimeStats,
          appendBlock(keyBlock, Liquid, isOwn = true, certChainStore = CertChainStore.empty).map(_ => {
            keyBlockIdsCache.put(keyBlock.uniqueId)
            blockchainUpdater.score
          })
        )
      }.executeOn(scheduler)
    } yield result

  protected[appender] def appendBlock(
      block: Block,
      blockType: BlockType,
      isOwn: Boolean = false,
      alreadyVerifiedTxIds: Set[ByteStr] = Set.empty,
      certChainStore: CertChainStore
  ): Either[ValidationError, Option[Int]] = {
    for {
      _ <- Either.cond(
        !blockchainUpdater.hasScript(block.sender.toAddress),
        (),
        BlockAppendError(s"Account '${block.sender.toAddress}' is scripted are therefore not allowed to forge blocks", block)
      )
      postAction        <- consensus.blockConsensusValidation(time.correctedTime(), block)
      maybeDiscardedTxs <- blockchainUpdater.processBlock(block, postAction, blockType, isOwn, alreadyVerifiedTxIds, certChainStore)
      _                    = utxStorage.removeAll(block.transactionData, mustBeInPool = isOwn)
      _                    = activePeerConnections.updateAttributesEstablishedChannels(timeStamp = time.getTimestamp(), blockchainUpdater)
      sendBackTransactions = collectSendBackTransactions(maybeDiscardedTxs)
      _ <- processCertChainStore(certChainStore, sendBackTransactions)
    } yield maybeDiscardedTxs.map(_ => blockchainUpdater.height)
  }

  protected def collectSendBackTransactions(maybeDiscardedTxs: Option[DiscardedTransactions]): Seq[Transaction] = {
    val sendBackTransactions = maybeDiscardedTxs.toSeq.flatten.map {
      case etx: ExecutedContractTransaction =>
        etx.tx
      case atomicTx: AtomicTransaction =>
        AtomicUtils.rollbackExecutedTxs(atomicTx)
      case tx =>
        tx
    }
    log.trace(s"Send '${sendBackTransactions.size}' discarded transactions back to UTX")
    sendBackTransactions
  }

  protected def processCertChainStore(certChainStore: CertChainStore, sendBackTransactions: Seq[Transaction]): Either[ValidationError, Unit] =
    Right(())

  def rollbackDroppedBlocks(reference: ByteStr, droppedBlocks: Seq[Block]): Either[ValidationError, Unit] = {
    log.warn(s"Rollback dropped blocks after '$reference': '$droppedBlocks'")

    for {
      _ <- blockchainUpdater.removeAfter(reference)
      _ <- droppedBlocks.toList.traverse { returningBlock =>
        consensus.calculatePostAction(returningBlock).flatMap { postAction =>
          blockchainUpdater
            .processBlock(returningBlock, postAction, Hard, certChainStore = CertChainStore.empty) // TODO: implement certs rollback in future???
        }
      }
    } yield ()
  }

  def sendTransactionsBackToUtx(droppedBlocks: DiscardedBlocks): Unit = BaseAppender.sendTransactionsBackToUtx(utxStorage, droppedBlocks)
}

object BaseAppender extends ScorexLogging {

  def sendTransactionsBackToUtx(utx: UtxPool, droppedBlocks: DiscardedBlocks): Unit = {
    val droppedTransactionsCount = droppedBlocks.view.map { droppedBlock =>
      droppedBlock.transactionData.view
        .map {
          case etx: ExecutedContractTransaction => etx.tx
          case tx                               => tx
        }
        .map(utx.putIfNew(_, None)) // todo: add certs
        .size
    }.sum

    log.debug(s"Sent '$droppedTransactionsCount' transactions from dropped blocks back to UTX")
  }

  sealed trait BlockType

  object BlockType {
    case object Liquid extends BlockType
    case object Hard   extends BlockType
  }
}
