package com.wavesenterprise

import monix.execution.Scheduler
import monix.execution.Scheduler.io
import monix.execution.schedulers.SchedulerService

object TestSchedulers extends Schedulers {
  private val scheduler: SchedulerService = io("tests-scheduler")

  override val blockLoaderScheduler: Scheduler               = scheduler
  override val microBlockLoaderScheduler: Scheduler          = scheduler
  override val syncChannelSelectorScheduler: Scheduler       = scheduler
  override val appenderScheduler: Scheduler                  = scheduler
  override val historyRepliesScheduler: Scheduler            = scheduler
  override val minerScheduler: Scheduler                     = scheduler
  override val anchoringScheduler: Scheduler                 = scheduler
  override val dockerExecutorScheduler: Scheduler            = scheduler
  override val transactionBroadcastScheduler: Scheduler      = scheduler
  override val policyScheduler: Scheduler                    = scheduler
  override val cryptoServiceScheduler: Scheduler             = scheduler
  override val utxPoolSyncScheduler: Scheduler               = scheduler
  override val discardingHandlerScheduler: Scheduler         = scheduler
  override val channelProcessingScheduler: Scheduler         = scheduler
  override val messageObserverScheduler: Scheduler           = scheduler
  override val contractExecutionMessagesScheduler: Scheduler = scheduler
  override val validatorResultsHandlerScheduler: Scheduler   = scheduler
  override val ntpTimeScheduler: Scheduler                   = scheduler
  override val utxPoolBackgroundScheduler: Scheduler         = scheduler
  override val blockchainUpdatesScheduler: Scheduler         = scheduler
  override val signaturesValidationScheduler: Scheduler      = scheduler
  override val blockVotesHandlerScheduler: Scheduler         = scheduler
  override val healthCheckScheduler: Scheduler               = scheduler
  override val consensualSnapshotScheduler: Scheduler        = scheduler
  override val apiComputationsScheduler: SchedulerService    = scheduler
  override val confidentialDataScheduler: SchedulerService   = scheduler
}
