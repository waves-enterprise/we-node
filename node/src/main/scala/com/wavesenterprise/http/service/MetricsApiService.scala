package com.wavesenterprise.http.service

import com.wavesenterprise.metrics.{Metrics, MetricsType}
import play.api.libs.json.{Json, OFormat}

class MetricsApiService {

  private[this] val metricsRegistry = Metrics.registry

  def getStatus: MetricsStatus = {
    val metrics  = MetricsType.values.toSet
    val enabled  = metricsRegistry.enabled
    val disabled = metrics -- enabled
    MetricsStatus(enabled, disabled)
  }

  def updateStatus(newStatus: MetricsStatus): Unit = {
    val MetricsStatus(enabled, disabled) = newStatus
    enabled.foreach(metricsRegistry.enable)
    disabled.foreach(metricsRegistry.disable)
  }
}

case class MetricsStatus(enabled: Set[MetricsType], disabled: Set[MetricsType])

object MetricsStatus {
  implicit val MetricsStatusFormat: OFormat[MetricsStatus] = Json.format
}
