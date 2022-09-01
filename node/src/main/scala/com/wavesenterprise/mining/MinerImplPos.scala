package com.wavesenterprise.mining

import cats.implicits._
import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.block.Block
import com.wavesenterprise.block.Block.NgBlockVersion
import com.wavesenterprise.consensus.{GeneratingBalanceProvider, PoSConsensus}
import com.wavesenterprise.docker.ContractExecutionComponents
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator
import com.wavesenterprise.metrics.{BlockStats, Instrumented}
import com.wavesenterprise.network.BlockLoader.LoaderState
import com.wavesenterprise.network._
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.{FunctionalitySettings, WESettings}
import com.wavesenterprise.state.NG
import com.wavesenterprise.state.appender.{BaseAppender, MicroBlockAppender}
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.utx.UtxPool
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler

import scala.concurrent.duration._

case class MinerImplPos(
    appender: BaseAppender,
    microBlockAppender: MicroBlockAppender,
    activePeerConnections: ActivePeerConnections,
    blockchainUpdater: BlockchainUpdater with NG,
    settings: WESettings,
    time: Time,
    utx: UtxPool,
    ownerKey: PrivateKeyAccount,
    consensus: PoSConsensus,
    loaderStateReporter: Coeval[LoaderState],
    transactionsAccumulatorProvider: TransactionsAccumulatorProvider,
    contractExecutionComponentsOpt: Option[ContractExecutionComponents],
    executableTransactionsValidatorOpt: Option[ExecutableTransactionsValidator]
)(implicit val scheduler: Scheduler)
    extends MinerBase
    with MinerDebugInfo
    with ScorexLogging
    with Instrumented {

  private val pos = consensus

  private val blockchainSettings = settings.blockchain

  override def state: MinerDebugInfo.State = debugState

  override def getNextBlockGenerationOffset(account: PrivateKeyAccount): Either[String, FiniteDuration] = {
    val height    = blockchainUpdater.height
    val lastBlock = blockchainUpdater.lastBlock.get

    for {
      _         <- checkAge(height, blockchainUpdater.lastBlockTimestamp.get)
      _         <- checkScript(account)
      timestamp <- nextBlockGenerationTime(blockchainSettings.custom.functionality, height, lastBlock, account)
      currentTime = time.correctedTime()
      _ <- checkMinerPermissions(account, currentTime)
      calculatedOffset = timestamp - currentTime
      offset           = Math.max(calculatedOffset, minerSettings.minimalBlockGenerationOffset.toMillis).millis
    } yield offset
  }

  private def nextBlockGenerationTime(fs: FunctionalitySettings, height: Int, block: Block, account: PublicKeyAccount): Either[String, Long] = {
    val balance = GeneratingBalanceProvider.balance(blockchainUpdater, fs, height, account.toAddress)

    def calcTime(height: Int, block: Block, account: PublicKeyAccount, balance: Long): Either[String, Long] = {
      for {
        blockPosData <- block.consensusData.asPoSMaybe().leftMap(_.err)
        expectedTimestamp <- pos
          .getValidBlockDelay(height, account.publicKey.getEncoded, blockPosData.baseTarget, balance)
          .map(_ + block.timestamp)
          .leftMap(_.toString)
        result <- Either.cond(
          0 < expectedTimestamp && expectedTimestamp < Long.MaxValue,
          expectedTimestamp,
          s"Invalid next block generation time: '$expectedTimestamp'"
        )
      } yield result
    }

    if (balance >= GeneratingBalanceProvider.MinimalEffectiveBalance) {
      calcTime(height, block, account, balance)
    } else {
      Left(
        s"Generating balance '$balance' of '$account' is lower than required '${GeneratingBalanceProvider.MinimalEffectiveBalance}' for generation")
    }
  }

  override protected def keyBlockGenerationLoop(account: PrivateKeyAccount, skipOffset: Boolean = false): Task[Unit] =
    Task
      .defer {
        getNextBlockGenerationOffset(account)
          .map { offset =>
            (if (skipOffset) {
               log.debug(s"Mining attempt for account '$account'")
               forgeBlock(account)
             } else {
               deferForgeBlock(account, offset)
             })
              .flatMap {
                case Right((estimators, keyBlock, totalConstraint)) =>
                  appender
                    .processMinedKeyBlock(keyBlock)
                    .asyncBoundary(scheduler)
                    .map {
                      case Right(score) =>
                        log.info(s"Forged and applied key-block '$keyBlock' by '$account' with cumulative score '$score'")
                        BlockStats.mined(keyBlock, blockchainUpdater.height)
                        executableTransactionsValidatorOpt.foreach(_.cancelValidation())
                        activePeerConnections.broadcast(BlockForged(keyBlock))
                        scheduleNextMining()
                        startMicroBlockMining(account, keyBlock, estimators, totalConstraint)
                      case Left(err) =>
                        log.warn(s"Error block appending: '$err'")
                    }

                case Left(err) =>
                  log.debug(s"No block generated because '$err', retrying")
                  keyBlockGenerationLoop(account)
              }

          }
          .valueOr(keyBlockSchedulingError)
      }
      .executeOn(scheduler)

  private def deferForgeBlock(account: PrivateKeyAccount,
                              offset: FiniteDuration): Task[Either[String, (MiningConstraints, Block, MiningConstraint)]] = {
    val resultOffset = {
      if (checkQuorumAvailable.isRight)
        offset
      else
        offset.max(quorumNotAvailableDelay)
    }

    log.debug(s"Next mining attempt for account '$account' in '${resultOffset.toMillis.millis.toCoarsest}'")
    forgeBlock(account).delayExecution(resultOffset)
  }

  private def forgeBlock(account: PrivateKeyAccount): Task[Either[String, (MiningConstraints, Block, MiningConstraint)]] = Task {
    val currentTime = time.correctedTime()
    // should take last block right at the time of mining since micro-blocks might have been added
    val height              = blockchainUpdater.height
    val lastBlock           = blockchainUpdater.lastBlock.get
    val referencedBlockInfo = blockchainUpdater.bestLastBlockInfo(currentTime - minMicroBlockDurationMills).get
    val refBlockTimestamp   = referencedBlockInfo.timestamp
    val refBlockId          = referencedBlockInfo.blockId

    lazy val blockDelay = currentTime - lastBlock.timestamp
    lazy val balance    = GeneratingBalanceProvider.balance(blockchainUpdater, blockchainSettings.custom.functionality, height, account.toAddress)

    measureSuccessful(
      blockBuildTimeStats,
      for {
        _ <- checkLoaderState
        _ <- checkQuorumAvailable
        refBlockInfo <- referencedBlockInfo.consensus
          .asPoSMaybe()
          .fold(e => Left(e.err), posData => Right(posData.baseTarget))
        validBlockDelay <- pos
          .getValidBlockDelay(height, account.publicKey.getEncoded, refBlockInfo, balance)
          .leftMap(_.toString)
          .ensure(s"Block delay '$blockDelay' was not less than estimated delay")(_ < blockDelay)
        _ = log.debug(
          s"Forging with '$account', time '$blockDelay' > estimated time '$validBlockDelay', balance '$balance', " +
            s"prev block '$refBlockId' at '$height' with target '$refBlockInfo'")
        consensusData <- pos
          .consensusData(
            account.publicKey.getEncoded,
            height,
            refBlockInfo,
            refBlockTimestamp,
            blockchainUpdater.parent(lastBlock, 2).map(_.timestamp),
            currentTime
          )
          .leftMap(_.toString)
        estimators = MiningConstraints(blockchainUpdater, height, settings.miner.maxBlockSizeInBytes, Some(minerSettings))
        keyBlock <- Block
          .buildAndSignKeyBlock(currentTime, refBlockId, consensusData, account, blockFeatures(NgBlockVersion))
          .leftMap(_.err)
      } yield (estimators, keyBlock, estimators.total)
    )
  }
}
