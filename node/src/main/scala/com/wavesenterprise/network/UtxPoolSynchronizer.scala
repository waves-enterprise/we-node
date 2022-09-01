package com.wavesenterprise.network

import com.wavesenterprise.settings.SynchronizationSettings.UtxSynchronizerSettings
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.{Cancelable, Scheduler}

/**
  * Observes incoming transactions, sends ones to the UTX pool and to all peers.
  */
class UtxPoolSynchronizer(
    settings: UtxSynchronizerSettings,
    incomingTransactions: ChannelObservable[TxWithSize],
    txBroadcaster: TxBroadcaster
)(implicit val scheduler: Scheduler)
    extends ScorexLogging
    with AutoCloseable {

  private val synchronization: Cancelable = {
    incomingTransactions
      .bufferTimedAndCounted(settings.maxBufferTime, settings.maxBufferSize)
      .mapParallelUnordered(Runtime.getRuntime.availableProcessors()) { batch =>
        Task {
          val toAdd = batch.map { case (channel, tx: TxWithSize) => TxFromChannel(channel, tx) }
          txBroadcaster.broadcastBatchIfNewWithSize(toAdd)
        }
      }
      .logErr
      .onErrorRestartUnlimited
      .doOnComplete(Task(log.info("Utx pool synchronizer stops")))
      .subscribe()
  }

  override def close(): Unit = {
    synchronization.cancel()
  }
}
