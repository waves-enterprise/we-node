package com.wavesenterprise.network.contracts

import kamon.Kamon
import kamon.metric.{CounterMetric, TimerMetric}

trait ConfidentialDataSynchronizerMetrics {
  protected val loadingTaskExecution: TimerMetric       = Kamon.timer("confidential-data-synchronizer.loading-task-execution")
  protected val decryptingConfidentialData: TimerMetric = Kamon.timer("confidential-data-synchronizer.decrypting-confidential-data")

  protected val inventoryIterations: CounterMetric =
    Kamon.counter("confidential-data-synchronizer.inventory-iterations")
  protected val inventoryRequests: CounterMetric =
    Kamon.counter("confidential-data-synchronizer.inventory-requests")

  protected val additionsToPending: CounterMetric  = Kamon.counter("confidential-data-synchronizer.additions-to-pending")
  protected val removalsFromPending: CounterMetric = Kamon.counter("confidential-data-synchronizer.removals-from-pending")
  protected val additionsToLost: CounterMetric     = Kamon.counter("confidential-data-synchronizer.additions-to-lost")
  protected val removalsFromLost: CounterMetric    = Kamon.counter("confidential-data-synchronizer.removals-from-lost")

}
