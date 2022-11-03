package com.wavesenterprise.state.appender

import cats.data.EitherT
import cats.implicits._
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.block.Block
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.consensus.Consensus
import com.wavesenterprise.docker.validator.{ContractValidatorResultsStore, ExecutableTransactionsValidator}
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator.ValidationStartCause.KeyBlockAppended
import com.wavesenterprise.metrics.{BlockStats, Instrumented, Metrics, _}
import com.wavesenterprise.mining.Miner
import com.wavesenterprise.network._
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.state.appender.BaseAppender.BlockType.Hard
import com.wavesenterprise.state.{Blockchain, _}
import com.wavesenterprise.transaction.ValidationError.{GenericError, InvalidSignature, PermissionError}
import com.wavesenterprise.transaction._
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Observable, OverflowStrategy}
import org.influxdb.dto.Point

import scala.util.{Left, Right}

/**
  * Observes the [[BlockLoader]] and append broadcast blocks and extension to the blockchain.
  */
class BlockAppender(
    baseAppender: BaseAppender,
    val blockchainUpdater: BlockchainUpdater with Blockchain,
    invalidBlocks: InvalidBlockStorage,
    miner: Miner,
    executableTransactionsValidatorOpt: Option[ExecutableTransactionsValidator],
    contractValidatorResultsStoreOpt: Option[ContractValidatorResultsStore],
    consensus: Consensus,
    signatureValidator: SignatureValidator,
    blockLoader: BlockLoader,
    permissionValidator: PermissionValidator,
    activePeerConnections: ActivePeerConnections,
)(implicit scheduler: Scheduler)
    extends ScorexLogging
    with Instrumented
    with AutoCloseable {

  private val blockReceivingLag = Kamon.histogram("block-receiving-lag")

  private def applyBroadcastBlock(channel: Channel, newBlock: Block): Task[Unit] = Task.defer {
    log.debug(s"Attempting to append broadcast block '$newBlock' from '${id(channel)}'")
    BlockStats.received(newBlock, BlockStats.Source.Broadcast, channel)
    blockReceivingLag.safeRecord(System.currentTimeMillis() - newBlock.timestamp)

    (for {
      _ <- EitherT.cond[Task](
        permissionValidator.validateMiner(blockchainUpdater, newBlock.signerData.generatorAddress, newBlock.timestamp).isRight,
        (),
        PermissionError(s"Block signer '${newBlock.signerData.generator}' does not have miner permission")
      )
      _ <- EitherT(signatureValidator.validate(newBlock))
      preValidatedTxIds <- EitherT.right[GenericError](
        signatureValidator.preValidateProvenTransactions(newBlock.transactionData).asyncBoundary(scheduler))
      maybeNewScore <- EitherT(baseAppender.processBroadcastKeyBlock(newBlock, preValidatedTxIds))
        .leftSemiflatMap(err => blockLoader.forceUpdate().as(err))
    } yield {
      maybeNewScore match {
        case Some(newScore) =>
          val height = blockchainUpdater.height
          BlockStats.applied(newBlock, BlockStats.Source.Broadcast, height)
          contractValidatorResultsStoreOpt.foreach(_.removeExceptFor(newBlock.uniqueId))
          log.info(s"Successfully appended broadcast block '$newBlock' by '${newBlock.sender}' from '${id(channel)}' with score '$newScore'")
          if (newBlock.transactionData.isEmpty) activePeerConnections.broadcast(BlockForged(newBlock), Set(channel))
          executableTransactionsValidatorOpt.foreach(_.runValidation(height, KeyBlockAppended(newBlock)))
          miner.scheduleNextMining()
        case None =>
          log.debug(s"Block '$newBlock' from '${id(channel)}' already in the state")
      }
    }).valueOr {
      case invalidSignature: InvalidSignature =>
        BlockStats.declined(newBlock, BlockStats.Source.Broadcast)
        closeChannel(channel, s"Could not append '$newBlock': '$invalidSignature'")
      case validationError =>
        BlockStats.declined(newBlock, BlockStats.Source.Broadcast)
        log.warn(s"Could not append '$newBlock' from '${id(channel)}': '$validationError'")
    }
  }

  protected def applyExtension(channel: Channel, extensionBlocks: Seq[BlockWrapper]): Task[Unit] = Task.defer {
    val (blocks, certChainStoresByBlockId) = extensionBlocks.foldRight((Seq.empty[Block], Map.empty[BlockId, CertChainStore])) {
      case (extensionBlock, (blocks, certStoresByBlockId)) =>
        extensionBlock match {
          case HistoryBlock(block, certStore) if CertChainStore.empty != certStore =>
            (block +: blocks, certStoresByBlockId + (block.uniqueId -> certStore))
          case _ =>
            (extensionBlock.block +: blocks, certStoresByBlockId)
        }
    }

    log.debug(s"Attempting to append extension '${formatBlocks(blocks)}' from '${id(channel)}'")
    blocks.foreach(BlockStats.received(_, BlockStats.Source.Extension, channel))

    (for {
      validBlocks                <- EitherT(signatureValidator.validateOrdered(blocks))
      blockIdToPreValidatedTxIds <- EitherT.right[GenericError](preValidateBlocksTxs(validBlocks).asyncBoundary(scheduler))
      processResult              <- EitherT.fromEither[Task](processValidExtensionBlocks(validBlocks, blockIdToPreValidatedTxIds, certChainStoresByBlockId))
    } yield {
      log.info(
        s"Successfully processed extension '${formatBlocks(blocks)}' from '${id(channel)}'. " +
          s"Added new blocks '${processResult.addedNewBlocks}', removed old blocks '${processResult.removedOldBlocks}'.")

      if (processResult.removedOldBlocks > 0) {
        Metrics.write(MetricsType.Block,
                      Point
                        .measurement("rollback")
                        .addField("depth", processResult.removedOldBlocks))
      }

      if (processResult.addedNewBlocks > 0) miner.scheduleNextMining()
    }).valueOr { error =>
      val message = s"Error appending extension '${formatBlocks(blocks)}' from '${id(channel)}': '$error'"
      log.warn(message)
      closeChannel(channel, message)
    }
  }

  private def preValidateBlocksTxs(validBlocks: List[Block]): Task[Map[ByteStr, Set[ByteStr]]] = {
    validBlocks
      .map { block =>
        signatureValidator
          .preValidateProvenTransactions(block.transactionData)
          .map(block.uniqueId -> _)
      }
      .parSequence
      .map(_.toMap)
  }

  private case class ExtensionProcessResult(addedNewBlocks: Int, removedOldBlocks: Int)

  private def processValidExtensionBlocks(blocks: List[Block],
                                          blockIdToPreValidatedTxIds: Map[BlockId, Set[ByteStr]],
                                          certChainStoresByBlockId: Map[BlockId, CertChainStore]): Either[ValidationError, ExtensionProcessResult] = {
    val newBlocks = blocks.dropWhile(blockchainUpdater.contains)

    newBlocks.headOption.map(_.reference) match {
      case None =>
        log.debug("No new blocks found in extension")
        Right(ExtensionProcessResult(0, 0))

      case Some(extensionReference) =>
        val initialHeight = blockchainUpdater.height

        for {
          commonBlockHeight <- blockchainUpdater
            .heightOf(extensionReference)
            .toRight(GenericError(s"Extension reference '$extensionReference' not found in the current state"))
          blocksCountToRemove = initialHeight - commonBlockHeight
          lastBlock     <- blockchainUpdater.lastBlock.toRight(GenericError("Last block not found"))
          _             <- consensus.checkExtensionRollback(blocksCountToRemove, lastBlock)
          droppedBlocks <- blockchainUpdater.removeAfter(extensionReference)
          _ <- appendExtensionBlocks(extensionReference, newBlocks, blockIdToPreValidatedTxIds, certChainStoresByBlockId).leftFlatMap { err =>
            baseAppender.rollbackDroppedBlocks(extensionReference, droppedBlocks) >> Left(err)
          }
          _ = baseAppender.sendTransactionsBackToUtx(droppedBlocks)
        } yield {
          ExtensionProcessResult(newBlocks.size, blocksCountToRemove)
        }
    }
  }

  private def appendExtensionBlocks(extensionReference: ByteStr,
                                    blocks: List[Block],
                                    blockIdToPreValidatedTxIds: Map[ByteStr, Set[ByteStr]],
                                    certChainStoresByBlockId: Map[BlockId, CertChainStore]): Either[ValidationError, Unit] = {
    blocks.zipWithIndex
      .traverse {
        case (block, i) =>
          val blockId              = block.uniqueId
          val alreadyVerifiedTxIds = blockIdToPreValidatedTxIds.getOrElse(blockId, Set.empty)
          baseAppender
            .appendBlock(block,
                         Hard,
                         alreadyVerifiedTxIds = alreadyVerifiedTxIds,
                         certChainStore = certChainStoresByBlockId.getOrElse(blockId, CertChainStore.empty))
            .map { maybeNewHeight =>
              maybeNewHeight.foreach(BlockStats.applied(block, BlockStats.Source.Extension, _))
            }
            .leftMap((block, i, _))
      }
      .leftMap {
        case (block, i, error) =>
          invalidBlocks.add(block.uniqueId, error)
          blocks.drop(i).foreach(BlockStats.declined(_, BlockStats.Source.Extension))
          log.warn(s"Processed '$i' of '${blocks.size}' extension blocks (reference '$extensionReference'), error appending block '$block': '$error'")
          error
      }
      .void
  }

  private val extensionApplying = blockLoader.extensionEvents
    .asyncBoundary(OverflowStrategy.BackPressure(2))
    .mapEval {
      case (ch, extension) =>
        applyExtension(ch, extension.blocks)
    }
    .onErrorHandle { ex =>
      log.error("Error during extension applying", ex)
    }

  private val broadcastBlockApplying = blockLoader.broadcastBlockEvents
    .asyncBoundary(OverflowStrategy.BackPressure(10))
    .mapEval {
      case (ch, bw) =>
        Task.defer {
          val block = bw.block
          if (blockchainUpdater.isRecentlyApplied(block.uniqueId)) {
            Task(log.debug(s"Block '$block' from '${id(ch)}' already in the state"))
          } else {
            applyBroadcastBlock(ch, block)
          }
        }
    }
    .onErrorHandle { ex =>
      log.error("Error during broadcast block applying", ex)
    }

  private val applying = Observable(extensionApplying, broadcastBlockApplying).merge.logErr.onErrorRestartUnlimited.subscribe()

  override def close(): Unit = {
    applying.cancel()
  }
}
