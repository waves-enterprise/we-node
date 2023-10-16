package com.wavesenterprise.network.contracts

import kamon.Kamon
import kamon.metric.{CounterMetric, TimerMetric}

trait ConfidentialDataInventoryMetrics {
  protected val incomingInventories: CounterMetric = Kamon.counter("confidential-data-inventory-handler.incoming-inventories")
  protected val filteredAndProcessedInventories: CounterMetric =
    Kamon.counter("confidential-data-inventory-handler.filtered-and-processed-inventories")
  protected val inventoryRequests: CounterMetric        = Kamon.counter("confidential-data-inventory-handler.inventory-requests")
  protected val inventoryRequestProcessing: TimerMetric = Kamon.timer("confidential-data-inventory-handler.inventory-request-processing")
}
