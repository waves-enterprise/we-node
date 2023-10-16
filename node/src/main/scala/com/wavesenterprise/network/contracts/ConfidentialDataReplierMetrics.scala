package com.wavesenterprise.network.contracts

import kamon.Kamon
import kamon.metric.{CounterMetric, TimerMetric}
import monix.eval.Task

trait ConfidentialDataReplierMetrics {

  protected val requestsCounter: CounterMetric  = Kamon.counter("confidential-data-replier.requests")
  private val responseCounter: CounterMetric    = Kamon.counter("confidential-data-replier.responses")
  protected val loadFromDB: TimerMetric         = Kamon.timer("confidential-data-replier.load-from-db")
  protected val responseEncrypting: TimerMetric = Kamon.timer("confidential-data-replier.response-encrypting")

  protected def countResponse(dataType: ConfidentialDataType): Task[Unit] = Task {
    val dataTypeString: String = dataType match {
      case ConfidentialDataType.Input  => "input"
      case ConfidentialDataType.Output => "output"
    }
    responseCounter.refine("dataType" -> dataTypeString)
  }
}
