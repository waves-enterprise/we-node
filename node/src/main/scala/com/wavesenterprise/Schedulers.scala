package com.wavesenterprise

import monix.execution.Scheduler

trait Schedulers {

  val blockLoaderScheduler: Scheduler
  val microBlockLoaderScheduler: Scheduler
  val syncChannelSelectorScheduler: Scheduler
  val appenderScheduler: Scheduler
  val historyRepliesScheduler: Scheduler
  val minerScheduler: Scheduler
  val anchoringScheduler: Scheduler
  val dockerExecutorScheduler: Scheduler
  val transactionBroadcastScheduler: Scheduler
  val policyScheduler: Scheduler
  val cryptoServiceScheduler: Scheduler
  val utxPoolSyncScheduler: Scheduler
  val discardingHandlerScheduler: Scheduler
  val channelProcessingScheduler: Scheduler
  val messageObserverScheduler: Scheduler
  val contractExecutionMessagesScheduler: Scheduler
  val validatorResultsHandlerScheduler: Scheduler
  val ntpTimeScheduler: Scheduler
  val utxPoolBackgroundScheduler: Scheduler
  val blockchainUpdatesScheduler: Scheduler
  val signaturesValidationScheduler: Scheduler
  val blockVotesHandlerScheduler: Scheduler
  val healthCheckScheduler: Scheduler
  val consensualSnapshotScheduler: Scheduler
  val apiComputationsScheduler: Scheduler
}
