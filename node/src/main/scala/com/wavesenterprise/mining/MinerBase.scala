package com.wavesenterprise.mining

import cats.data.EitherT
import cats.implicits._
import cats.kernel.Monoid
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.acl.Role
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, MicroBlock, TxMicroBlock, VoteMicroBlock}
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.consensus.{CftLikeConsensusBlockData, Vote}
import com.wavesenterprise.database.certs.CertificatesState
import com.wavesenterprise.docker.{ContractExecutionComponents, MinerTransactionsExecutor}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.metrics.{BlockStats, Instrumented, _}
import com.wavesenterprise.mining.Miner._
import com.wavesenterprise.network.BlockLoader.LoaderState
import com.wavesenterprise.network.BlockLoader.LoaderState.ExpectingBlocks
import com.wavesenterprise.network._
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.{MinerSettings, WESettings}
import com.wavesenterprise.state.appender.MicroBlockAppender
import com.wavesenterprise.state.diffs.TransactionDiffer.TransactionValidationError
import com.wavesenterprise.state.reader.CompositeBlockchain
import com.wavesenterprise.state.{ByteStr, Diff, NG}
import com.wavesenterprise.transaction.{BlockchainUpdater, Transaction, ValidationError}
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.utx.UtxPool
import kamon.Kamon
import kamon.metric.{HistogramMetric, MeasurementUnit}
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler
import monix.execution.cancelables.SerialCancelable
import monix.reactive.{Consumer, Observable}

import java.security.cert.X509Certificate
import scala.concurrent.duration._
import scala.util.{Left, Right}

trait MinerBase extends Miner with Instrumented with ScorexLogging {

  protected def activePeerConnections: ActivePeerConnections
  protected def time: Time
  protected def blockchainUpdater: BlockchainUpdater with NG
  protected def settings: WESettings
  protected def ownerKey: PrivateKeyAccount
  protected def microBlockAppender: MicroBlockAppender
  protected def loaderStateReporter: Coeval[LoaderState]
  protected def transactionsAccumulatorProvider: TransactionsAccumulatorProvider
  protected def contractExecutionComponentsOpt: Option[ContractExecutionComponents]
  protected def utx: UtxPool
  protected def scheduler: Scheduler

  protected val minerSettings: MinerSettings            = settings.miner
  protected val quorumNotAvailableDelay: FiniteDuration = minerSettings.noQuorumMiningDelay
  protected val minMicroBlockDurationMills: Long        = minerSettings.minMicroBlockAge.toMillis
  protected val blockBuildTimeStats: HistogramMetric    = Kamon.histogram("pack-and-forge-block-time", MeasurementUnit.time.milliseconds)

  @volatile protected var debugState: MinerDebugInfo.State = MinerDebugInfo.Disabled

  private val blockGeneration      = SerialCancelable()
  private val microBlockGeneration = SerialCancelable()
  private val txsConfirmation      = SerialCancelable()

  private val liquidBlockBuildTimeStats    = Kamon.timer("forge-liquid-block-time")
  private val microBlockBuildTimeStats     = Kamon.timer("forge-micro-block-time")
  private val microBlockFullBuildTimeStats = Kamon.timer("full-forge-micro-block-time")

  override def scheduleNextMining(): Unit = {
    log.debug(s"Mining scheduled for account '$ownerKey'")
    Miner.blockMiningStarted.increment()

    txsConfirmation      := SerialCancelable()
    microBlockGeneration := SerialCancelable()
    blockGeneration      := keyBlockGenerationLoop(ownerKey).runAsyncLogErr(scheduler)

    debugState = MinerDebugInfo.MiningBlocks
  }

  protected def initNewConfirmation(keyBlockId: BlockId): MinerTransactionsConfirmatory = {
    val transactionsAccumulator = transactionsAccumulatorProvider.build()
    val transactionsExecutorOpt = contractExecutionComponentsOpt.map(_.createMinerExecutor(transactionsAccumulator, keyBlockId))

    contractExecutionComponentsOpt.foreach(_.setDelegatingState(transactionsAccumulator))

    buildTransactionConfirmatory(transactionsAccumulator, transactionsExecutorOpt, blockchainUpdater, time)
  }

  protected def buildTransactionConfirmatory(transactionsAccumulator: TransactionsAccumulator,
                                             transactionsExecutorOpt: Option[MinerTransactionsExecutor],
                                             blockchain: CertificatesState,
                                             time: Time): MinerTransactionsConfirmatory =
    new MinerTransactionsConfirmatory(
      transactionsAccumulator = transactionsAccumulator,
      transactionExecutorOpt = transactionsExecutorOpt,
      utx = utx,
      pullingBufferSize = settings.miner.pullingBufferSize,
      utxCheckDelay = settings.miner.utxCheckDelay,
      ownerKey = ownerKey
    )(scheduler)

  protected def startMicroBlockMining(account: PrivateKeyAccount,
                                      keyBlock: Block,
                                      constraints: MiningConstraints,
                                      totalConstraint: MiningConstraint): Unit = {
    Miner.microMiningStarted.increment()

    val confirmatory = initNewConfirmation(keyBlock.uniqueId)

    microBlockGeneration := generateMicroBlockSequence(
      confirmatory = confirmatory,
      account = account,
      keyBlock = keyBlock,
      constraintsDescriptor = constraints,
      totalConstraint = totalConstraint,
      parallelLiquidGeneration = parallelLiquidGenActivation
    ).runAsyncLogErr(scheduler)

    txsConfirmation := confirmatory.confirmationTask.runAsyncLogErr(scheduler)

    log.info(s"Micro-block mining started for account '$account'")
  }

  protected def parallelLiquidGenActivation: Boolean = {
    val currentHeight = blockchainUpdater.height
    blockchainUpdater.activatedFeatures
      .get(BlockchainFeature.ParallelLiquidBlockGenerationSupport.id)
      .exists(_ <= currentHeight)
  }

  override def close(): Unit = {
    blockGeneration.cancel()
    microBlockGeneration.cancel()
  }

  protected def checkAge(parentHeight: Int, parentTimestamp: Long): Either[String, Unit] = {
    lazy val blockAge = (time.correctedTime() - parentTimestamp).millis

    if (parentHeight != 1 && checkBlockAge(blockAge)) {
      Left(s"Block chain is too old (last block timestamp is '$parentTimestamp' generated '$blockAge' ago)")
    } else {
      Right(())
    }
  }

  private def checkBlockAge(blockAge: FiniteDuration): Boolean = {
    blockAge > minerSettings.intervalAfterLastBlockThenGenerationIsAllowed
  }

  protected def checkScript(account: PrivateKeyAccount): Either[String, Unit] = {
    if (blockchainUpdater.hasScript(account.toAddress)) {
      Left(s"Account '$account' is scripted and therefore not allowed to forge blocks")
    } else {
      Right(())
    }
  }

  protected def checkQuorumAvailable: Either[String, Unit] = {
    val minersCount = activePeerConnections.minersCount()
    if (minersCount < minerSettings.quorum) {
      Left(s"Quorum not available '$minersCount/${minerSettings.quorum}', not forging block")
    } else {
      Right(())
    }
  }

  protected def checkMinerPermissions(account: PrivateKeyAccount, timestamp: Long): Either[String, Unit] = {
    if (blockchainUpdater.permissions(account.toAddress).contains(Role.Miner, timestamp)) {
      Right(())
    } else {
      Left(s"Current account '$account' is not miner")
    }
  }

  protected def checkLoaderState: Either[String, Unit] = {
    Either.cond(!loaderStateReporter().isInstanceOf[ExpectingBlocks], (), "Extension is loading, mining not allowed")
  }

  protected def blockFeatures(version: Byte): Set[Short] = {
    if (version <= 2) {
      Set.empty[Short]
    } else {
      val featuresToSupport: Set[Short] = settings.features.featuresSupportMode
        .fold(_.supportedFeatures.toSet.filter(BlockchainFeature.implemented), _ => BlockchainFeature.implemented)

      featuresToSupport
        .filterNot(settings.blockchain.custom.functionality.preActivatedFeatures.keySet)
        .filterNot(blockchainUpdater.approvedFeatures.keySet)
    }
  }

  /**
    * Generates key-block based on current consensus.
    */
  protected def keyBlockGenerationLoop(account: PrivateKeyAccount, withoutOffset: Boolean = false): Task[Unit]

  private sealed trait MicroblockGenerationEvent
  private object CheckTimeoutEvent                               extends MicroblockGenerationEvent
  private case class NewTxEvent(txWithDiff: TransactionWithDiff) extends MicroblockGenerationEvent
  private case class MicroblockGenerationAccumulator(liquidBlock: Block,
                                                     txs: List[Transaction],
                                                     diff: Diff,
                                                     constraint: MultiDimensionalMiningConstraint,
                                                     lastMicroTime: Long)

  protected def generateMicroBlockSequence(confirmatory: MinerTransactionsConfirmatory,
                                           account: PrivateKeyAccount,
                                           keyBlock: Block,
                                           constraintsDescriptor: MiningConstraints,
                                           totalConstraint: MiningConstraint,
                                           parallelLiquidGeneration: Boolean): Task[Unit] = {
    val microBlockIntervalMillis   = settings.miner.microBlockInterval.toMillis
    val checkTimeoutEventFrequency = (settings.miner.microBlockInterval / 10).max(100.millis).min(500.millis)

    def initAccumulator = MicroblockGenerationAccumulator(
      keyBlock,
      List.empty[Transaction],
      Diff.empty,
      MultiDimensionalMiningConstraint(totalConstraint, constraintsDescriptor.micro),
      time.correctedTime()
    )

    Task(debugState = MinerDebugInfo.MiningMicroblocks) >>
      Observable(
        confirmatory.confirmedTxsStream.map(NewTxEvent),
        Observable.repeat(CheckTimeoutEvent).delayOnNext(checkTimeoutEventFrequency)
      ).merge.consumeWith {
        buildMicroblockGenerationConsumer(confirmatory,
                                          initAccumulator,
                                          account,
                                          constraintsDescriptor,
                                          microBlockIntervalMillis,
                                          parallelLiquidGeneration)
      }.void
  }

  private def buildMicroblockGenerationConsumer(
      confirmatory: MinerTransactionsConfirmatory,
      initAccumulator: => MicroblockGenerationAccumulator,
      account: PrivateKeyAccount,
      constraintsDescriptor: MiningConstraints,
      microBlockIntervalMillis: Long,
      parallelLiquidGeneration: Boolean
  ): Consumer[MicroblockGenerationEvent, MicroblockGenerationAccumulator] = {
    Consumer.foldLeftTask[MicroblockGenerationAccumulator, MicroblockGenerationEvent](initAccumulator) {
      case (acc @ MicroblockGenerationAccumulator(accLiquidBlock, accTxs, accDiff, accConstraint, lastMicroTime), event) =>
        Task.defer {
          val certs = accDiff.certByDnHash.values
          event match {
            case CheckTimeoutEvent =>
              if (accTxs.nonEmpty && time.correctedTime() - lastMicroTime >= microBlockIntervalMillis) {
                val restTotalConstraint = accConstraint.constraints.head
                generateAndAppendMicroBlock(confirmatory,
                                            account,
                                            accLiquidBlock,
                                            accTxs.reverse,
                                            certs,
                                            restTotalConstraint,
                                            parallelLiquidGeneration)
                  .flatMap(processMicroblockGenerationResult(constraintsDescriptor, acc, restTotalConstraint))
              } else {
                Task.pure(acc)
              }
            case NewTxEvent(txWithDiff @ TransactionWithDiff(tx, diff)) =>
              val updatedState      = CompositeBlockchain.composite(blockchainUpdater, accDiff)
              val updatedConstraint = accConstraint.put(updatedState, tx)

              if (updatedConstraint.isOverfilled) {
                val restTotalConstraint = accConstraint.constraints.head
                generateAndAppendMicroBlock(confirmatory,
                                            account,
                                            accLiquidBlock,
                                            accTxs.reverse,
                                            certs,
                                            restTotalConstraint,
                                            parallelLiquidGeneration)
                  .flatMap(processMicroblockGenerationResult(constraintsDescriptor, acc, restTotalConstraint, Some(txWithDiff)))
              } else {
                Task.pure {
                  MicroblockGenerationAccumulator(accLiquidBlock, tx :: accTxs, Monoid.combine(accDiff, diff), updatedConstraint, lastMicroTime)
                }
              }
          }
        }
    }
  }

  private def processMicroblockGenerationResult(
      constraintsDescriptor: MiningConstraints,
      accumulator: MicroblockGenerationAccumulator,
      newTotalConstraint: MiningConstraint,
      maybeRestTx: Option[TransactionWithDiff] = None)(result: MicroBlockMiningResult): Task[MicroblockGenerationAccumulator] = {
    result match {
      case Success(newLiquidBlock, _) =>
        Task {
          val initNextConstraint = MultiDimensionalMiningConstraint(newTotalConstraint, constraintsDescriptor.micro)
          val (nextConstraint, nextTxAcc, nextDiff) = maybeRestTx.fold((initNextConstraint, List.empty[Transaction], Diff.empty)) {
            case TransactionWithDiff(tx, diff) =>
              val updatedState      = CompositeBlockchain.composite(blockchainUpdater, diff)
              val updatedConstraint = initNextConstraint.put(updatedState, tx)
              (updatedConstraint, List(tx), diff)
          }

          MicroblockGenerationAccumulator(newLiquidBlock, nextTxAcc, nextDiff, nextConstraint, time.correctedTime())
        }
      case Miner.Retry(delay) =>
        Task.pure(accumulator).delayResult(delay)
      case Miner.Stop =>
        Task {
          txsConfirmation      := SerialCancelable()
          microBlockGeneration := SerialCancelable()
          debugState = MinerDebugInfo.MiningBlocks
          log.info("Micro-block mining completed")
          accumulator
        }
      case Error(error) =>
        Task {
          debugState = MinerDebugInfo.Error(error.toString)
          log.warn(s"Error mining micro-block: '$error'")
          accumulator
        }.delayResult(settings.miner.microBlockInterval)
    }
  }

  /**
    * Generates micro-block based on current consensus.
    */
  protected def generateAndAppendMicroBlock(confirmatory: MinerTransactionsConfirmatory,
                                            account: PrivateKeyAccount,
                                            liquidBlock: Block,
                                            transactions: Seq[Transaction],
                                            certs: Iterable[X509Certificate],
                                            restTotalConstraint: MiningConstraint,
                                            parallelLiquidGeneration: Boolean): Task[MicroBlockMiningResult] = Task.defer {
    log.trace(s"Generating micro-block for account '$account', constraints '$restTotalConstraint'")

    buildAndAppendMicroBlock(account, liquidBlock, time.correctedTime(), Left(transactions), certs, parallelLiquidGeneration)
      .flatMap {
        case success @ Success(_, microBlock) =>
          Task(confirmatory.transactionExecutorOpt.foreach(_.onMicroBlockMined(microBlock))) *>
            taskFromChannelGroupFuture(activePeerConnections.broadcast(buildInventory(account, success.microBlock)))
              .map { channelSet =>
                log.trace(
                  s"Successful transactions micro-block '${microBlock.totalLiquidBlockSig}' inventory broadcast " +
                    s"to channels ${channelSet.map(id).mkString("'", ", ", "'")}"
                )

                if (restTotalConstraint.isFull) {
                  log.debug(s"Stop forging micro-blocks, the block is full: '$restTotalConstraint'")
                  Stop
                } else {
                  success
                }
              }

        case other => Task.pure(other)
      }
  }

  protected def buildAndAppendMicroBlock(account: PrivateKeyAccount,
                                         liquidBlock: Block,
                                         currentTs: Long,
                                         txsOrVotes: Either[Seq[Transaction], Seq[Vote]],
                                         certs: Iterable[X509Certificate],
                                         parallelLiquidGeneration: Boolean): Task[MicroBlockMiningResult] = Task.defer {
    log.trace(s"Accumulated ${txsOrVotes.fold(txs => s"'${txs.size}' txs", votes => s"'${votes.size}' votes")} for micro-block")
    (for {
      microBlockBuildingResult <- EitherT {
        measureTask(
          microBlockFullBuildTimeStats,
          buildMicroBlockWithAdditions(account, liquidBlock, currentTs, txsOrVotes, certs, parallelLiquidGeneration)
        )
      }
      (microBlock, liquidBlock, maybeCertStore) = microBlockBuildingResult
      _ <- EitherT(microBlockAppender.appendMicroBlock(microBlock, isOwn = true, certChainStore = maybeCertStore))
    } yield {
      log.info(s"Forged and applied micro-block '$microBlock' by '$account'")
      TimingStats.microBlockTime(microBlock)
      BlockStats.mined(microBlock)
      Success(liquidBlock, microBlock)
    }).valueOr {
      case err: TransactionValidationError =>
        utx.removeAll(Map(err.tx -> err.cause.toString))
        Error(err)
      case err =>
        Error(err)
    }
  }

  protected def buildInventory(account: PrivateKeyAccount, microBlock: MicroBlock): MicroBlockInventory = {
    val inventoryV2FeatureActivated = blockchainUpdater.activatedFeatures
      .get(BlockchainFeature.MicroBlockInventoryV2Support.id)
      .exists(_ <= blockchainUpdater.height)

    blockchainUpdater.currentBaseBlock match {
      case Some(keyBlock) if inventoryV2FeatureActivated =>
        MicroBlockInventoryV2(keyBlock.uniqueId, account, microBlock.totalLiquidBlockSig, microBlock.prevLiquidBlockSig)
      case _ =>
        MicroBlockInventoryV1(account, microBlock.totalLiquidBlockSig, microBlock.prevLiquidBlockSig)
    }
  }

  private def buildLiquidBlock(account: PrivateKeyAccount,
                               liquidBlock: Block,
                               txsOrVotes: Either[Seq[Transaction], Seq[Vote]]): Either[ValidationError.GenericError, Block] = {
    measureSuccessful(
      liquidBlockBuildTimeStats, {
        val liquidBlockTransactions = liquidBlock.transactionData ++ txsOrVotes.fold(identity, _ => Seq.empty)
        val liquidBlockConsensusData = (liquidBlock.consensusData, txsOrVotes) match {
          case (CftLikeConsensusBlockData(_, overallSkippedRounds), Right(microBlockVotes)) =>
            CftLikeConsensusBlockData(microBlockVotes, overallSkippedRounds)
          case (otherConsensusData, _) => otherConsensusData
        }

        Block.buildAndSign(
          version = Block.NgBlockVersion,
          timestamp = liquidBlock.timestamp,
          reference = liquidBlock.reference,
          consensusData = liquidBlockConsensusData,
          transactionData = liquidBlockTransactions,
          signer = account,
          featureVotes = liquidBlock.featureVotes
        )
      }
    )
  }

  private def buildMicroBlock(account: PrivateKeyAccount,
                              liquidBlock: Block,
                              currentTs: Long,
                              txsOrVotes: Either[Seq[Transaction], Seq[Vote]],
                              totalSignature: ByteStr): Either[ValidationError, MicroBlock] = {
    measureSuccessful(
      microBlockBuildTimeStats, {
        txsOrVotes match {
          case Left(txs) =>
            val version = if (totalSignature.arr.isEmpty) TxMicroBlock.ModifiedTotalSignVersion else TxMicroBlock.DefaultVersion

            TxMicroBlock.buildAndSign(
              generator = account,
              timestamp = currentTs,
              transactionData = txs,
              prevResBlockSig = liquidBlock.signerData.signature,
              totalResBlockSig = totalSignature,
              version = version
            )
          case Right(votes) =>
            val version = if (totalSignature.arr.isEmpty) VoteMicroBlock.ModifiedTotalSignVersion else VoteMicroBlock.DefaultVersion

            VoteMicroBlock.buildAndSign(
              generator = account,
              timestamp = currentTs,
              votes = votes,
              prevResBlockSig = liquidBlock.signerData.signature,
              totalResBlockSig = totalSignature,
              version = version
            )
        }
      }
    )
  }

  private def buildMicroBlockWithAdditions(account: PrivateKeyAccount,
                                           liquidBlock: Block,
                                           currentTs: Long,
                                           txsOrVotes: Either[Seq[Transaction], Seq[Vote]],
                                           certs: Iterable[X509Certificate],
                                           parallelLiquidGeneration: Boolean): Task[Either[ValidationError, (MicroBlock, Block, CertChainStore)]] = {

    def buildLiquidBlockTask: Task[Either[ValidationError, Block]] = Task(buildLiquidBlock(account, liquidBlock, txsOrVotes))
    def buildMicroBlockTask(totalSignature: ByteStr = ByteStr.empty): Task[Either[ValidationError, MicroBlock]] =
      Task(buildMicroBlock(account, liquidBlock, currentTs, txsOrVotes, totalSignature))
    def buildCertStoreTask: Task[Either[ValidationError, CertChainStore]] =
      Task {
        if (certs.isEmpty)
          Right(CertChainStore.empty)
        else
          CertChainStore.fromCertificates(certs.toVector).leftMap(ValidationError.fromCryptoError)
      }

    if (parallelLiquidGeneration) {
      (buildLiquidBlockTask, buildMicroBlockTask(), buildCertStoreTask).parMapN {
        case (newLiquidResult, newMicroWithoutSignatureResult, certChainResult) =>
          for {
            newLiquid                <- newLiquidResult
            newMicroWithoutSignature <- newMicroWithoutSignatureResult
            maybeChainStore          <- certChainResult
          } yield {
            (newMicroWithoutSignature.withTotalSignature(newLiquid.uniqueId), newLiquid, maybeChainStore)
          }
      }
    } else {
      (for {
        newLiquid       <- EitherT(buildLiquidBlockTask)
        newMicro        <- EitherT(buildMicroBlockTask(newLiquid.uniqueId))
        maybeChainStore <- EitherT(buildCertStoreTask)
      } yield {
        (newMicro, newLiquid, maybeChainStore)
      }).value
    }
  }

  protected def keyBlockSchedulingError(error: String): Task[Unit] = {
    log.debug(s"Not scheduling block mining because '$error'")
    debugState = MinerDebugInfo.Error(error)
    Task.unit
  }

}
