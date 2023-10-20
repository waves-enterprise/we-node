package com.wavesenterprise.mining

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.{Block, MicroBlock}
import com.wavesenterprise.consensus.{BlockVotesHandler, CftConsensus, Consensus, PoAConsensus, PoSConsensus}
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.docker.ContractExecutionComponents
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator
import com.wavesenterprise.network.BlockLoader.LoaderState
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.privacy.PolicyDataSynchronizer
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.state.NG
import com.wavesenterprise.state.appender.{BaseAppender, MicroBlockAppender}
import com.wavesenterprise.state.contracts.confidential.ConfidentialStateUpdater
import com.wavesenterprise.transaction._
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utx.UtxPool
import kamon.Kamon
import kamon.metric.CounterMetric
import monix.eval.Coeval
import monix.execution.Scheduler

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

trait Miner extends AutoCloseable {
  def scheduleNextMining(): Unit
}

trait MinerDebugInfo {
  def state: MinerDebugInfo.State

  def getNextBlockGenerationOffset(account: PrivateKeyAccount): Either[String, FiniteDuration]
}

object MinerDebugInfo {

  sealed trait State

  case object MiningBlocks extends State

  case object MiningMicroblocks extends State

  case object Disabled extends State

  case class Error(error: String) extends State

}

object Miner {
  val blockMiningStarted: CounterMetric = Kamon.counter("block-mining-started")
  val microMiningStarted: CounterMetric = Kamon.counter("micro-mining-started")

  val MaxTransactionsPerMicroblock: Int = 6000
  val MaxVotesPerMicroblock: Int        = 500
  val MinUtxCheckDelay: FiniteDuration  = 100.milliseconds

  val Disabled: Miner with MinerDebugInfo = new Miner with MinerDebugInfo {
    override def scheduleNextMining(): Unit = ()

    override def getNextBlockGenerationOffset(account: PrivateKeyAccount): Either[String, FiniteDuration] = Left("Disabled")

    override val state: MinerDebugInfo.Disabled.type = MinerDebugInfo.Disabled

    override def close(): Unit = ()
  }

  def apply(appender: BaseAppender,
            microBlockAppender: MicroBlockAppender,
            activePeerConnections: ActivePeerConnections,
            transactionsAccumulatorProvider: TransactionsAccumulatorProvider,
            blockchainUpdater: BlockchainUpdater with NG,
            settings: WESettings,
            timeService: Time,
            utx: UtxPool,
            ownerKey: PrivateKeyAccount,
            consensus: Consensus,
            contractExecutionComponentsOpt: Option[ContractExecutionComponents],
            executableTransactionsValidatorOpt: Option[ExecutableTransactionsValidator],
            loaderStateReporter: Coeval[LoaderState],
            policyDataSynchronizer: PolicyDataSynchronizer,
            votesHandler: => BlockVotesHandler,
            confidentialRocksDBStorage: ConfidentialRocksDBStorage,
            confidentialStateUpdater: ConfidentialStateUpdater)(implicit scheduler: Scheduler): Miner with MinerDebugInfo = {
    consensus match {
      case pos: PoSConsensus =>
        MinerImplPos(
          appender = appender,
          microBlockAppender = microBlockAppender,
          activePeerConnections = activePeerConnections,
          blockchainUpdater = blockchainUpdater,
          settings = settings,
          time = timeService,
          utx = utx,
          ownerKey = ownerKey,
          consensus = pos,
          loaderStateReporter = loaderStateReporter,
          transactionsAccumulatorProvider = transactionsAccumulatorProvider,
          contractExecutionComponentsOpt = contractExecutionComponentsOpt,
          executableTransactionsValidatorOpt = executableTransactionsValidatorOpt,
          confidentialRocksDBStorage = confidentialRocksDBStorage,
          confidentialStateUpdater = confidentialStateUpdater
        )
      case poa: PoAConsensus =>
        MinerImplPoa(
          appender = appender,
          microBlockAppender = microBlockAppender,
          activePeerConnections = activePeerConnections,
          blockchainUpdater = blockchainUpdater,
          settings = settings,
          time = timeService,
          utx = utx,
          ownerKey = ownerKey,
          consensus = poa,
          loaderStateReporter = loaderStateReporter,
          transactionsAccumulatorProvider = transactionsAccumulatorProvider,
          contractExecutionComponentsOpt = contractExecutionComponentsOpt,
          executableTransactionsValidatorOpt = executableTransactionsValidatorOpt,
          confidentialRocksDBStorage = confidentialRocksDBStorage,
          confidentialStateUpdater = confidentialStateUpdater
        )
      case cft: CftConsensus =>
        MinerImplCft(
          appender = appender,
          microBlockAppender = microBlockAppender,
          activePeerConnections = activePeerConnections,
          blockchainUpdater = blockchainUpdater,
          settings = settings,
          time = timeService,
          utx = utx,
          ownerKey = ownerKey,
          consensus = cft,
          loaderStateReporter = loaderStateReporter,
          transactionsAccumulatorProvider = transactionsAccumulatorProvider,
          contractExecutionComponentsOpt = contractExecutionComponentsOpt,
          executableTransactionsValidatorOpt = executableTransactionsValidatorOpt,
          votesHandler = votesHandler,
          confidentialRocksDBStorage = confidentialRocksDBStorage,
          confidentialStateUpdater = confidentialStateUpdater
        )
    }
  }

  sealed trait MicroBlockMiningResult extends Product with Serializable

  case object Stop                                                  extends MicroBlockMiningResult
  case class Retry(delay: FiniteDuration)                           extends MicroBlockMiningResult
  case class Error(e: ValidationError)                              extends MicroBlockMiningResult
  case class Success(newLiquidBlock: Block, microBlock: MicroBlock) extends MicroBlockMiningResult
}
