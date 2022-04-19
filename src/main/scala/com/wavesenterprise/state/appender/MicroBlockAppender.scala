package com.wavesenterprise.state.appender

import cats.data.EitherT
import com.wavesenterprise.block.{MicroBlock, TxMicroBlock, VoteMicroBlock}
import com.wavesenterprise.consensus.Consensus
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator.ValidationStartCause.MicroBlockAppended
import com.wavesenterprise.metrics.{BlockStats, Instrumented}
import com.wavesenterprise.network.MicroBlockLoader.ReceivedMicroBlock
import com.wavesenterprise.network._
import com.wavesenterprise.network.privacy.PrivacyMicroBlockHandler
import com.wavesenterprise.state.{Blockchain, ByteStr, NG, SignatureValidator}
import com.wavesenterprise.transaction.ValidationError.InvalidSignature
import com.wavesenterprise.transaction.{BlockchainUpdater, ValidationError}
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.utx.UtxPool
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.OverflowStrategy

/**
  * Applies new micro-block to NG when last liquid block matches with received micro-block.
  */
class MicroBlockAppender(
    blockchainUpdater: BlockchainUpdater with Blockchain with NG,
    utxStorage: UtxPool,
    signatureValidator: SignatureValidator,
    consensus: Consensus,
    microBlockLoader: MicroBlockLoader,
    privacyMicroBlockHandler: PrivacyMicroBlockHandler,
    executableTransactionsValidatorOpt: Option[ExecutableTransactionsValidator],
)(implicit scheduler: Scheduler)
    extends ScorexLogging
    with AutoCloseable
    with Instrumented {
  private val microBlockProcessingTimeStats = Kamon.timer("microblock-processing-time")
  private val microPreValidationTimeStats   = Kamon.timer("microblock-pre-validation-time")

  private val updates = microBlockLoader.loadingUpdates
    .asyncBoundary(OverflowStrategy.Default)
    .combineLatestMap(blockchainUpdater.lastBlockInfo) { (newMicroEvent, lastLiquidBlock) =>
      if (newMicroEvent.microBlock.prevLiquidBlockSig == lastLiquidBlock.id)
        Some(newMicroEvent)
      else
        microBlockLoader.storage.findMicroBlockByPrevSign(lastLiquidBlock.id)
    }

  private val microBlockApplying = updates
    .collect { case Some(event) => event }
    .distinctUntilChanged
    .mapEval(processLoadedMicroBlock)
    .logErr
    .onErrorRestartUnlimited
    .subscribe()

  private def processLoadedMicroBlock(entry: ReceivedMicroBlock): Task[Unit] = {
    import entry.microBlock

    (for {
      validMicroBlock   <- EitherT(signatureValidator.validate(microBlock))
      preValidatedTxIds <- EitherT.right[ValidationError](preValidateMicroBlockTxs(microBlock).asyncBoundary(scheduler))
      _                 <- EitherT(Task(consensus.microBlockConsensusValidation(microBlock)))
      _                 <- EitherT(appendMicroBlock(validMicroBlock, alreadyVerifiedTxIds = preValidatedTxIds))
    } yield {
      log.info(s"Loaded micro-block '$validMicroBlock' by '${validMicroBlock.sender.toAddress}' from '${id(entry.channel)}' has been appended")
      BlockStats.applied(microBlock)

      blockchainUpdater.currentBaseBlock.foreach { baseBlock =>
        executableTransactionsValidatorOpt.foreach { validator =>
          validator.runValidation(blockchainUpdater.height, MicroBlockAppended(baseBlock, validMicroBlock.totalLiquidBlockSig))
        }
      }
    }).valueOr {
      case invalidSignature: InvalidSignature =>
        BlockStats.declined(microBlock)
        closeChannel(entry.channel, s"Could not append loaded micro-block '${microBlock.totalLiquidBlockSig}': '$invalidSignature'")
      case validationError =>
        BlockStats.declined(microBlock)
        log.warn(s"Could not append loaded micro-block '${microBlock.totalLiquidBlockSig}' from '${id(entry.channel)}': '$validationError'")
    }
  }

  private def preValidateMicroBlockTxs(microBlock: MicroBlock): Task[Set[ByteStr]] = {
    microBlock match {
      case txMicroBlock: TxMicroBlock =>
        measureTask(microPreValidationTimeStats, signatureValidator.preValidateProvenTransactions(txMicroBlock.transactionData))
      case _: VoteMicroBlock =>
        Task.pure(Set.empty)
    }
  }

  def appendMicroBlock(
      microBlock: MicroBlock,
      isOwn: Boolean = false,
      alreadyVerifiedTxIds: Set[ByteStr] = Set.empty
  ): Task[Either[ValidationError, Unit]] = {
    def measuredAction: Either[ValidationError, Unit] = {
      blockchainUpdater.processMicroBlock(microBlock, isOwn, alreadyVerifiedTxIds).map { _ =>
        microBlock match {
          case txMicro: TxMicroBlock => utxStorage.removeAll(txMicro.transactionData, mustBeInPool = isOwn)
          case _: VoteMicroBlock     => ()
        }
      }
    }

    for {
      result <- Task {
        measureSuccessful(microBlockProcessingTimeStats, measuredAction)
      }.executeOn(scheduler)

      _ <- if (isOwn) {
        privacyMicroBlockHandler.broadcastInventoryIfNeed(microBlock)
      } else {
        Task.unit
      }

    } yield result
  }

  override def close(): Unit =
    microBlockApplying.cancel()
}
