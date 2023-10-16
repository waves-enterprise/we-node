package com.wavesenterprise.mining

import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.PoALikeConsensus.ConsensusRoundInfo
import com.wavesenterprise.consensus.{ConsensusBlockData, PoAConsensus, PoALikeConsensusBlockData}
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.docker.ContractExecutionComponents
import com.wavesenterprise.docker.validator.ExecutableTransactionsValidator
import com.wavesenterprise.mining.PoaLikeMiner.GenericMinerError
import com.wavesenterprise.network.BlockLoader.LoaderState
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.state.NG
import com.wavesenterprise.state.appender.{BaseAppender, MicroBlockAppender}
import com.wavesenterprise.state.contracts.confidential.ConfidentialStateUpdater
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utx.UtxPool
import monix.eval.Coeval
import monix.execution.Scheduler

import scala.concurrent.duration._

case class MinerImplPoa(
    appender: BaseAppender,
    microBlockAppender: MicroBlockAppender,
    activePeerConnections: ActivePeerConnections,
    blockchainUpdater: BlockchainUpdater with NG,
    settings: WESettings,
    time: Time,
    utx: UtxPool,
    ownerKey: PrivateKeyAccount,
    consensus: PoAConsensus,
    loaderStateReporter: Coeval[LoaderState],
    transactionsAccumulatorProvider: TransactionsAccumulatorProvider,
    contractExecutionComponentsOpt: Option[ContractExecutionComponents],
    executableTransactionsValidatorOpt: Option[ExecutableTransactionsValidator],
    confidentialRocksDBStorage: ConfidentialRocksDBStorage,
    confidentialStateUpdater: ConfidentialStateUpdater
)(implicit val scheduler: Scheduler)
    extends PoaLikeMiner {

  override protected val maxKeyBlockGenerationDelay: FiniteDuration = (consensus.roundAndSyncMillis / 3).millis

  override protected def keyBlockData(roundInfo: ConsensusRoundInfo, referencedBlock: Block): Either[GenericMinerError, ConsensusBlockData] = {
    referencedBlock.blockHeader.consensusData
      .asPoAMaybe()
      .bimap(
        err => GenericMinerError(err.toString),
        referenceBlockData => PoALikeConsensusBlockData(referenceBlockData.overallSkippedRounds + roundInfo.skippedRounds)
      )
  }
}
