package com.wavesenterprise

import com.wavesenterprise.utils.ScorexLogging
import monix.execution.Scheduler.{computation, forkJoin, global, singleThread}
import monix.execution.schedulers.{ExecutorScheduler, SchedulerService}

import scala.concurrent.Await
import scala.concurrent.duration._

class AppSchedulers extends ScorexLogging with Schedulers {

  import AppSchedulers._

  private val availableProcessors = Runtime.getRuntime.availableProcessors()

  override val blockLoaderScheduler: SchedulerService = singleThread("block-loader", reporter = log.error("Error in Block Loader", _))

  override val microBlockLoaderScheduler: SchedulerService =
    computation(name = "micro-block-loader", reporter = log.error("Error in Microblock Loader", _))

  override val syncChannelSelectorScheduler: SchedulerService =
    singleThread("sync-channel-selector", reporter = log.error("Error in Sync Channel Selector", _))

  override val appenderScheduler: SchedulerService = singleThread("appender", reporter = log.error("Error in Appender", _))

  override val historyRepliesScheduler: SchedulerService =
    computation(name = "history-replier", reporter = log.error("Error in History Replier", _))

  override val minerScheduler: SchedulerService = computation(name = "miner-pool", reporter = log.error("Error in Miner", _))

  override val anchoringScheduler: SchedulerService = singleThread("anchoring-pool", reporter = log.error("Error in Anchoring", _))

  override val dockerExecutorScheduler: SchedulerService =
    forkJoin(availableProcessors, 64, "docker-executor-pool", reporter = log.error("Error in Docker Executor", _))

  override val transactionBroadcastScheduler: SchedulerService =
    computation(name = "transaction-broadcaster", reporter = log.error("Error in Transaction Broadcaster", _))

  override val policyScheduler: SchedulerService =
    computation(name = "policy-pool", reporter = log.error("Error in Policy Pool", _))

  override val cryptoServiceScheduler: SchedulerService = computation(name = "crypto-service", reporter = log.error("Error in Crypto Api Service", _))

  override val utxPoolSyncScheduler: SchedulerService = computation(name = "utx-pool-sync", reporter = log.error("Error in UTX pool synchronizer", _))

  override val discardingHandlerScheduler: SchedulerService = singleThread("discarding-handler")

  override val channelProcessingScheduler: SchedulerService = singleThread("channel-processing-pool")

  override val messageObserverScheduler: SchedulerService = computation(name = "message-observer", parallelism = 2)

  override val contractExecutionMessagesScheduler: SchedulerService = singleThread("contract-execution-messages-sync-pool")

  override val validatorResultsHandlerScheduler: SchedulerService = singleThread("contract-validator-results-handler-pool")

  override val ntpTimeScheduler: SchedulerService = singleThread(name = "time-impl")

  override val utxPoolBackgroundScheduler: SchedulerService = singleThread("utx-pool-background")

  override val blockchainUpdatesScheduler: SchedulerService = singleThread("blockchain-updates-publisher")

  override val signaturesValidationScheduler: SchedulerService =
    computation(name = "sig-validator", reporter = log.error("Error in Signature Validator", _))

  override val blockVotesHandlerScheduler: SchedulerService = singleThread("block-votes-handler-pool")

  override val healthCheckScheduler: SchedulerService = computation(name = "health-check-pool")

  override val consensualSnapshotScheduler: SchedulerService =
    computation(name = "snapshot-scheduler", reporter = log.error("Error in snapshot scheduler", _))

  override val apiComputationsScheduler: SchedulerService = computation(name = "api-computations-pool")

  def shutdown(mode: ShutdownMode.Mode = ShutdownMode.FULL_SHUTDOWN): Unit = {
    shutdownAndWait(historyRepliesScheduler, "HistoryReplier")

    // UTX
    shutdownAndWait(utxPoolSyncScheduler, "UtxPoolSync")
    shutdownAndWait(utxPoolBackgroundScheduler, "UtxPoolBackground")

    shutdownAndWait(discardingHandlerScheduler, "DiscardingHandler")
    shutdownAndWait(channelProcessingScheduler, "ChannelsPool")
    shutdownAndWait(messageObserverScheduler, "MessageObserver", 3.minutes, tryForce = false)

    // Privacy
    shutdownAndWait(policyScheduler, "PolicyScheduler", tryForce = false)

    // Crypto
    shutdownAndWait(cryptoServiceScheduler, "CryptoService")

    // Docker
    shutdownAndWait(contractExecutionMessagesScheduler, "ContractExecutionMessages", tryForce = false)
    shutdownAndWait(validatorResultsHandlerScheduler, "ContractValidatorResultsHandler", tryForce = false)
    shutdownAndWait(dockerExecutorScheduler, "DockerExecutor", 3.minutes, tryForce = false)

    shutdownAndWait(anchoringScheduler, "Anchoring")
    shutdownAndWait(minerScheduler, "Miner", 3.minutes, tryForce = false)
    shutdownAndWait(microBlockLoaderScheduler, "MicroBlockLoader")
    shutdownAndWait(syncChannelSelectorScheduler, "SyncChannelSelector")
    shutdownAndWait(blockLoaderScheduler, "BlockLoader", tryForce = false)
    shutdownAndWait(appenderScheduler, "Appender", 3.minutes, tryForce = false)

    shutdownAndWait(transactionBroadcastScheduler, "TransactionReBroadcastScheduler")
    shutdownAndWait(blockchainUpdatesScheduler, "BlockchainUpdatesScheduler")
    shutdownAndWait(signaturesValidationScheduler, "SignaturesValidation")
    shutdownAndWait(ntpTimeScheduler, "NtpTime")
    shutdownAndWait(blockVotesHandlerScheduler, "BlockVotesHandler")

    if (mode == ShutdownMode.FULL_SHUTDOWN) {
      shutdownAndWait(healthCheckScheduler, "HealthCheckScheduler")
      shutdownAndWait(consensualSnapshotScheduler, "ConsensualSnapshotScheduler")
    }
  }
}

object AppSchedulers extends ScorexLogging {

  def shutdownAndWait(scheduler: SchedulerService, name: String, timeout: FiniteDuration = 1.minute, tryForce: Boolean = true): Unit = {
    log.debug(s"Shutting down $name scheduler")
    scheduler match {
      case es: ExecutorScheduler if tryForce => es.executor.shutdownNow()
      case s                                 => s.shutdown()
    }
    val r = Await.result(scheduler.awaitTermination(timeout, global), 2 * timeout)
    if (r)
      log.info(s"$name scheduler was shutdown successfully")
    else
      log.warn(s"Failed to shutdown $name scheduler properly during timeout")
  }
}
