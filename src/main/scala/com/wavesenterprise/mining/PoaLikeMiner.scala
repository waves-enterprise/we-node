package com.wavesenterprise.mining

import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.Block
import com.wavesenterprise.block.Block.NgBlockVersion
import com.wavesenterprise.consensus.PoALikeConsensus.{ConsensusRoundInfo, RoundTimestamps}
import com.wavesenterprise.consensus.{ConsensusBlockData, PoALikeConsensus}
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator
import com.wavesenterprise.metrics.{BlockStats, TimingStats}
import com.wavesenterprise.mining.Miner.{MicroBlockMiningResult, Stop}
import com.wavesenterprise.network.BlockForged
import com.wavesenterprise.state.NG
import com.wavesenterprise.state.appender.BaseAppender
import com.wavesenterprise.transaction.{BlockchainUpdater, Transaction}
import com.wavesenterprise.utils.{ScorexLogging, Time}
import monix.eval.Task

import scala.concurrent.duration.{FiniteDuration, _}

trait PoaLikeMiner extends MinerBase with MinerDebugInfo with ScorexLogging {
  def blockchainUpdater: BlockchainUpdater with NG
  def appender: BaseAppender
  def consensus: PoALikeConsensus
  def executableTransactionsValidatorOpt: Option[ExecutableTransactionsValidator]
  def time: Time

  import PoaLikeMiner._

  @volatile private var lastCheckedRound: Option[RoundTimestamps] = None

  override def state: MinerDebugInfo.State = debugState

  protected def maxKeyBlockGenerationDelay: FiniteDuration

  def getNextBlockGenerationOffset(account: PrivateKeyAccount): Either[String, FiniteDuration] = {
    val currentTimestamp   = time.correctedTime()
    val nextBlockTimestamp = consensus.nextRoundTimestamp(currentTimestamp)
    Right(
      PoALikeConsensus
        .calculatePeriodicDelay(currentTimestamp, nextBlockTimestamp, consensus.roundAndSyncMillis, maxDelay = maxKeyBlockGenerationDelay))
  }

  protected def keyBlockGenerationLoop(account: PrivateKeyAccount, skipOffset: Boolean = false): Task[Unit] =
    Task
      .defer {
        val height = blockchainUpdater.height
        (for {
          _ <- checkAge(height, blockchainUpdater.lastBlockTimestamp.get)
          _ <- checkScript(account)
        } yield {
          (if (skipOffset) {
             log.debug(s"Mining attempt for account '$account'")
             forgeBlock(account)
           } else {
             deferForgeBlock(account)
           })
            .flatMap {
              case Right((estimators, keyBlock, totalConstraint)) =>
                appender
                  .processMinedKeyBlock(keyBlock)
                  .asyncBoundary(scheduler)
                  .flatMap {
                    case Right(score) =>
                      Task {
                        log.info(s"Forged and applied key-block '$keyBlock' by '${account.address}' with cumulative score '$score', broadcasting it")
                        val forgedBlockRoundTimestamps = consensus.calculateRoundTimestamps(keyBlock.timestamp)
                        lastCheckedRound = Some(forgedBlockRoundTimestamps)
                        BlockStats.mined(keyBlock, blockchainUpdater.height)
                        TimingStats.keyBlockTime(keyBlock)
                        executableTransactionsValidatorOpt.foreach(_.cancelValidation())
                        activePeerConnections.broadcast(BlockForged(keyBlock))
                        scheduleNextMining()
                        startMicroBlockMining(account, keyBlock, estimators, totalConstraint)
                      }
                    case Left(err) =>
                      Task(log.warn(s"Error block appending: '$err'"))
                  }

              case Left(NotMinersTurn(desc)) =>
                log.trace(s"No block generated because '$desc', retrying")
                keyBlockGenerationLoop(account)

              case Left(GenericMinerError(err)) =>
                log.debug(s"No block generated because '$err', retrying")
                keyBlockGenerationLoop(account)

              case Left(RetryRequest(cause)) =>
                log.warn(s"Retrying block generation because: $cause")
                keyBlockGenerationLoop(account, skipOffset = true)
            }
        }).valueOr(keyBlockSchedulingError)
      }
      .executeOn(scheduler)

  private def deferForgeBlock(account: PrivateKeyAccount): Task[Either[PoaMinerError, (MiningConstraints, Block, MiningConstraint)]] = {
    val currentTs                  = time.correctedTime()
    val currentRoundEnd            = consensus.calculateRoundTimestamps(currentTs).roundEnd
    val currentRoundAlreadyChecked = lastCheckedRound.exists(_.roundEnd == currentRoundEnd)
    val offset = PoALikeConsensus.calculatePeriodicDelay(currentTs,
                                                         currentRoundEnd,
                                                         consensus.roundAndSyncMillis,
                                                         currentRoundAlreadyChecked,
                                                         maxDelay = maxKeyBlockGenerationDelay)

    log.debug(s"Next mining attempt for account '$account' in '${offset.toMillis.millis.toCoarsest}'")
    forgeBlock(account).delayExecution(offset)
  }

  override protected def generateAndAppendMicroBlock(confirmatory: MinerTransactionsConfirmatory,
                                                     account: PrivateKeyAccount,
                                                     liquidBlock: Block,
                                                     transactions: Seq[Transaction],
                                                     restTotalConstraint: MiningConstraint,
                                                     parallelLiquidGeneration: Boolean): Task[MicroBlockMiningResult] = {
    val currentTime     = math.max(time.correctedTime(), liquidBlock.timestamp)
    val roundTimestamps = consensus.calculateRoundTimestamps(liquidBlock.timestamp)

    def checkTsBoundsRes: Boolean =
      consensus
        .checkTxMicroblockTimestampBounds(currentTime, roundTimestamps)
        .isRight

    if (!checkTsBoundsRes) {
      Task.now(Stop)
    } else {
      super.generateAndAppendMicroBlock(confirmatory, account, liquidBlock, transactions, restTotalConstraint, parallelLiquidGeneration)
    }
  }

  private def forgeBlock(account: PrivateKeyAccount): Task[Either[PoaMinerError, (MiningConstraints, Block, MiningConstraint)]] = Task {

    /**
      * Well, need to check if there is a block in current round
      * 1. Get current round timestamps;
      * 2. Check if current best block's (could be liquid) timestamp is >= than round start;
      * 3. If the last best block is old, try forging;
      */
    val currentTime            = time.correctedTime()
    val currentRoundTimestamps = consensus.calculateRoundTimestamps(currentTime)

    if (lastCheckedRound.contains(currentRoundTimestamps)) {
      Left(NotMinersTurn("Current round already checked"))
    } else {
      val lastBlockMaybe = blockchainUpdater.lastBlock
      lastBlockMaybe match {
        case Some(lastBlock) if lastBlock.timestamp >= currentRoundTimestamps.roundStart =>
          lastCheckedRound = Some(currentRoundTimestamps)
          Left(NotMinersTurn("Current round's block is being mined"))

        case Some(referencedBlock) =>
          val height                                    = blockchainUpdater.height
          val refBlockTimestamp                         = referencedBlock.timestamp
          val refBlockID                                = referencedBlock.uniqueId
          val RoundTimestamps(roundStart, syncStart, _) = currentRoundTimestamps
          measureSuccessful(
            blockBuildTimeStats,
            for {
              _ <- checkLoaderState.leftMap(GenericMinerError.apply)
              _ <- Either.cond(roundStart > referencedBlock.timestamp, (), NotMinersTurn("New round has not started yet"))
              _ <- Either.cond(currentTime < syncStart, (), GenericMinerError("Mining is not allowed during sync period"))
              _ <- checkMinerPermissions(account, currentTime).leftMap(GenericMinerError.apply)
              roundInfo <- consensus
                .currentMinerAndSkippedRounds(refBlockTimestamp, currentTime, height)
                .leftMap(err => GenericMinerError(err.toString))
              _ = log.trace(
                s"[forgeBlock] Round info at '$currentRoundTimestamps': '$roundInfo', reference timestamp: '$refBlockTimestamp', current height: '$height'")
              _ <- Either.cond(account.toAddress == roundInfo.expectedMiner,
                               (),
                               NotMinersTurn(s"The current round belongs to another miner '${roundInfo.expectedMiner}'"))
              _ <- checkQuorumAvailable.leftMap(GenericMinerError)
              _ <- checkConsensusForgingRequirement(referencedBlock, roundStart)
              estimators = MiningConstraints(blockchainUpdater, height, settings.miner.maxBlockSizeInBytes, Some(minerSettings))
              blockData <- keyBlockData(roundInfo, referencedBlock)
              keyBlock <- Block
                .buildAndSignKeyBlock(currentTime, refBlockID, blockData, account, blockFeatures(NgBlockVersion))
                .leftMap(ex => GenericMinerError(ex.err))
              _ = log.trace(s"[forgeBlock] Forged key block '${keyBlock.uniqueId}' -> '$refBlockID'")
              _ = TimingStats.roundTimestamps(currentRoundTimestamps)
            } yield (estimators, keyBlock, estimators.total)
          )

        case None =>
          val errorMessage = "Impossible fatal error: last block is not found in blockchain"
          log.error(errorMessage)
          throw new IllegalStateException(errorMessage)
      }
    }
  }

  protected def keyBlockData(roundInfo: ConsensusRoundInfo, referencedBlock: Block): Either[GenericMinerError, ConsensusBlockData]

  protected def checkConsensusForgingRequirement(lastBlock: Block, roundStart: Long): Either[PoaMinerError, Unit] = Right(())
}

object PoaLikeMiner {
  sealed trait PoaMinerError                        extends Product with Serializable
  case class GenericMinerError(description: String) extends PoaMinerError
  case class NotMinersTurn(description: String)     extends PoaMinerError
  case class RetryRequest(cause: String)            extends PoaMinerError
}
