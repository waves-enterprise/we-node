package com.wavesenterprise.mining

import cats.data.EitherT
import cats.implicits._
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.block.Block
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.consensus.PoALikeConsensus.RoundTimestamps
import com.wavesenterprise.consensus._
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.docker.ContractExecutionComponents
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator
import com.wavesenterprise.mining.Miner.Success
import com.wavesenterprise.mining.PoaLikeMiner.{GenericMinerError, NotMinersTurn, PoaMinerError, RetryRequest}
import com.wavesenterprise.network.BlockLoader.LoaderState
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{id, taskFromChannelGroupFuture}
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.state.NG
import com.wavesenterprise.state.appender.{BaseAppender, MicroBlockAppender}
import com.wavesenterprise.state.contracts.confidential.ConfidentialStateUpdater
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{BlockchainUpdater, Transaction}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utx.UtxPool
import monix.catnap.Semaphore
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler
import monix.execution.cancelables.SerialCancelable
import monix.reactive.{Observable, OverflowStrategy}

import java.security.cert.X509Certificate
import scala.concurrent.duration.{DurationLong, FiniteDuration}

case class MinerImplCft(
    appender: BaseAppender,
    microBlockAppender: MicroBlockAppender,
    activePeerConnections: ActivePeerConnections,
    blockchainUpdater: BlockchainUpdater with NG,
    settings: WESettings,
    time: Time,
    utx: UtxPool,
    ownerKey: PrivateKeyAccount,
    consensus: CftConsensus,
    loaderStateReporter: Coeval[LoaderState],
    transactionsAccumulatorProvider: TransactionsAccumulatorProvider,
    contractExecutionComponentsOpt: Option[ContractExecutionComponents],
    executableTransactionsValidatorOpt: Option[ExecutableTransactionsValidator],
    votesHandler: BlockVotesHandler,
    confidentialRocksDBStorage: ConfidentialRocksDBStorage,
    confidentialStateUpdater: ConfidentialStateUpdater
)(implicit val scheduler: Scheduler)
    extends PoaLikeMiner {

  private[this] val voting                      = SerialCancelable()
  private[this] val microBlockBuildingAsyncLock = Semaphore.unsafe[Task](1)

  private val maxVotingDelay: FiniteDuration = (consensus.votingMillis / 3).millis

  override protected val maxKeyBlockGenerationDelay: FiniteDuration = (consensus.roundAndSyncMillis / 7).millis

  override def scheduleNextMining(): Unit = {
    super.scheduleNextMining()

    val votingTask =
      Task {
        val roundTs = blockchainUpdater.lastBlockTimestamp.getOrElse(time.correctedTime())
        consensus.calculateRoundTimestamps(roundTs)
      } flatMap { roundTimestamps =>
        votingLoop(roundTimestamps)
      }

    voting := votingTask.runAsyncLogErr(scheduler)
  }

  override def close(): Unit = {
    votesHandler.close()
    super.close()
  }

  override protected def checkConsensusForgingRequirement(lastBlock: Block, roundStart: Long): Either[PoaMinerError, Unit] = {
    if (
      lastBlock.consensusData.asCftMaybe().exists(_.isFinalized) ||
      Block.GenesisBlockVersions.contains(lastBlock.version)
    ) {
      Right(())
    } else if (roundStart + consensus.finalizationTimeoutMillis < time.correctedTime()) {
      discardNg()
      Left(RetryRequest(s"Last block '${lastBlock.uniqueId}' finalization timeout, NG has been reset"))
    } else {
      Left(NotMinersTurn(s"Last block '${lastBlock.uniqueId}' not finalized yet, retrying"))
    }
  }

  private def discardNg(): Unit = {
    blockchainUpdater.currentBaseBlock.foreach { baseBlock =>
      blockchainUpdater.removeAfter(baseBlock.reference).map { droppedBlocks =>
        BaseAppender.sendTransactionsBackToUtx(utx, droppedBlocks)
      }
    }
  }

  override protected def startMicroBlockMining(account: PrivateKeyAccount,
                                               keyBlock: Block,
                                               constraints: MiningConstraints,
                                               totalConstraint: MiningConstraint): Unit = {
    votesHandler.clear()
    super.startMicroBlockMining(account, keyBlock, constraints, totalConstraint)
  }

  private def votingLoop(roundTimestamps: RoundTimestamps): Task[Unit] = Task.defer {
    val currentTimestamp = time.correctedTime()
    val votingStart      = roundTimestamps.syncStart

    val timeOffset = PoALikeConsensus.calculatePeriodicDelay(currentTimestamp, votingStart, consensus.roundTillSyncMillis, maxDelay = maxVotingDelay)

    log.debug(s"Next voting attempt for account '$ownerKey' in '${timeOffset.toMillis.millis.toCoarsest}'")
    votingAttempt(roundTimestamps).delayExecution(timeOffset).flatMap {
      case Right(_) =>
        Task(log.debug("Voting completed").asRight)
      case Left(NotMinersTurn(desc)) =>
        log.trace(s"No voting because '$desc', retrying")
        votingLoop(roundTimestamps)
      case Left(GenericMinerError(err)) =>
        log.debug(s"No voting because '$err', retrying")
        votingLoop(roundTimestamps)
      case Left(RetryRequest(cause)) =>
        log.warn(s"Retry voting, because: '$cause'")
        votingLoop(roundTimestamps)
    }
  }

  private def votingAttempt(roundTimestamps: RoundTimestamps): Task[Either[PoaMinerError, Unit]] = {
    microBlockBuildingAsyncLock.withPermit {
      EitherT(Task {
        val currentTime = time.correctedTime()

        for {
          _         <- Either.cond(currentTime >= roundTimestamps.syncStart, (), NotMinersTurn("Synchronization time has not yet begun"))
          _         <- Either.cond(currentTime <= finalizationEnd(roundTimestamps), (), NotMinersTurn("Finalization time is over"))
          _         <- Either.cond(blockchainUpdater.height > 1, (), NotMinersTurn("Genesis block does not require voting"))
          lastBlock <- blockchainUpdater.lastBlock.toRight(GenericMinerError("Last block not found"))
          expectedValidators <- consensus
            .expectedValidators(lastBlock.reference, currentTime, lastBlock.sender.toAddress)
            .leftMap(err => GenericMinerError(s"Failed to determine the current validators: $err"))
        } yield (expectedValidators, lastBlock)
      }).flatMap {
        case (expectedValidators, lastBlock) if lastBlock.sender == ownerKey =>
          EitherT(tryMinerVoting(expectedValidators, lastBlock, roundTimestamps))
        case (expectedValidators, _) =>
          EitherT.right[PoaMinerError] {
            blockchainUpdater.lastBlockInfo
              .asyncBoundary(OverflowStrategy.Default)
              .takeWhile(_ => time.correctedTime() < finalizationEnd(roundTimestamps))
              .mapEval { lastBlockInfo =>
                tryValidatorVoting(expectedValidators, lastBlockInfo.id)
              }
              .completedL
          }
      }.value
    }
  }

  private sealed trait VoteAccumulationEvent
  private object CheckTimeoutEvent         extends VoteAccumulationEvent
  private case class VoteEvent(vote: Vote) extends VoteAccumulationEvent

  private def tryMinerVoting(expectedValidators: Set[Address],
                             liquidBlock: Block,
                             roundTimestamps: RoundTimestamps): Task[Either[PoaMinerError, Unit]] = {
    val checkTimeoutEventFrequency = (settings.miner.microBlockInterval / 10).max(100.millis).min(500.millis)
    val finalizationEndTs          = finalizationEnd(roundTimestamps)
    val fullVoteSetAwaitingEndTs   = fullVoteSetAwaitingEnd(roundTimestamps)

    Observable(
      votesHandler.blockVotes(liquidBlock.votingHash()).asyncBoundary(OverflowStrategy.Default).map(VoteEvent),
      Observable.repeat(CheckTimeoutEvent).delayOnNext(checkTimeoutEventFrequency)
    ).merge
      .takeWhile(_ => time.correctedTime() < finalizationEndTs)
      .foldWhileLeft(Map.empty[Address, Vote]) {
        case (acc, CheckTimeoutEvent) =>
          val isVotingEnd = votingEndCondition(expectedValidators, fullVoteSetAwaitingEndTs, acc.size)
          Either.cond(isVotingEnd, acc, acc)
        case (acc, VoteEvent(vote)) =>
          val voteAddress = vote.sender.toAddress
          val updatedAcc = {
            if (expectedValidators.contains(voteAddress))
              acc.updated(voteAddress, vote)
            else
              acc
          }

          val accumulatedVoteCount = updatedAcc.size
          val isVotingEnd          = votingEndCondition(expectedValidators, fullVoteSetAwaitingEndTs, accumulatedVoteCount)
          log.trace(s"Accumulated '$accumulatedVoteCount' votes: ${updatedAcc.values.mkString("'", "', '", "'")}'")

          Either.cond(isVotingEnd, updatedAcc, updatedAcc)
      }
      .mapEval { votesMap =>
        val currentTime = time.correctedTime()
        val result = EitherT.fromEither[Task] {
          consensus
            .checkVoteMicroBlockTimestampBounds(currentTime, roundTimestamps)
            .leftMap(err => GenericMinerError(err.toString))
        } *> EitherT(buildAndProcessVoteMicroBlock(liquidBlock, votesMap, currentTime))

        result.value
      }
      .firstL
  }

  private def buildAndProcessVoteMicroBlock(liquidBlock: Block,
                                            votesMap: Map[Address, Vote],
                                            currentTime: Long): Task[Either[GenericMinerError, Unit]] =
    buildAndAppendMicroBlock(ownerKey, liquidBlock, currentTime, Right(votesMap.values.toSeq), Iterable.empty, parallelLiquidGenActivation)
      .flatMap {
        case Success(_, microBlock) =>
          taskFromChannelGroupFuture(activePeerConnections.broadcast(buildInventory(ownerKey, microBlock))).map { channelSet =>
            Right {
              log.trace(
                s"Successful vote micro-block '${microBlock.totalLiquidBlockSig}' inventory broadcast " +
                  s"to channels ${channelSet.map(id(_)).mkString("'", ", ", "'")}"
              )
            }
          }
        case nonSuccess =>
          Task.pure(Left(GenericMinerError(s"Failed to build and append vote micro-block: $nonSuccess")))
      }

  private def votingEndCondition(expectedValidators: Set[Address], fullVoteSetAwaitingEndTs: Long, accumulatedVoteCount: Int): Boolean = {
    val fullVoteSetCondition     = accumulatedVoteCount == expectedValidators.size || time.correctedTime() >= fullVoteSetAwaitingEndTs
    val requiredVoteSetCondition = (accumulatedVoteCount + 1).toDouble / (expectedValidators.size + 1) > CftConsensus.CftRatio

    fullVoteSetCondition && requiredVoteSetCondition
  }

  private def tryValidatorVoting(expectedValidators: Set[Address], lastBlockId: BlockId): Task[Unit] = {
    Task {
      (for {
        _     <- Either.cond(expectedValidators.contains(ownerKey.toAddress), (), GenericError("The current node owner is not validator for this round"))
        block <- blockchainUpdater.liquidBlockById(lastBlockId).toRight(GenericError(s"Last block '$lastBlockId' not found"))
        vote  <- Vote.buildAndSign(ownerKey, block.votingHash())
      } yield {
        votesHandler.broadcastVote(vote)
        log.debug(s"Vote for the block '${block.uniqueId}' with hash '${block.votingHash()}' sent successfully")
      }).valueOr { cause =>
        log.debug(s"Vote not sent because '$cause'")
      }
    }
  }

  override protected def keyBlockData(roundInfo: PoALikeConsensus.ConsensusRoundInfo,
                                      referencedBlock: Block): Either[GenericMinerError, ConsensusBlockData] = {
    referencedBlock.blockHeader.consensusData
      .asCftMaybe()
      .bimap(
        err => GenericMinerError(err.toString),
        referenceBlockData => CftLikeConsensusBlockData(Seq.empty, referenceBlockData.overallSkippedRounds + roundInfo.skippedRounds)
      )
  }

  override def generateAndAppendMicroBlock(confirmatory: MinerTransactionsConfirmatory,
                                           account: PrivateKeyAccount,
                                           liquidBlock: Block,
                                           transactions: Seq[Transaction],
                                           certs: Iterable[X509Certificate],
                                           restTotalConstraint: MiningConstraint,
                                           parallelLiquidGeneration: Boolean): Task[Miner.MicroBlockMiningResult] =
    microBlockBuildingAsyncLock.withPermit {
      super.generateAndAppendMicroBlock(confirmatory, account, liquidBlock, transactions, certs, restTotalConstraint, parallelLiquidGeneration)
    }

  @inline private def finalizationEnd(roundTimestamps: RoundTimestamps): Long =
    roundTimestamps.roundEnd + consensus.finalizationTimeoutMillis

  @inline private def fullVoteSetAwaitingEnd(roundTimestamps: RoundTimestamps): Long =
    roundTimestamps.syncStart + consensus.fullVoteSetTimeoutMillis

}
