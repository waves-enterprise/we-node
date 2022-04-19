package com.wavesenterprise.metrics

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

class MetricsRegistry {

  private[this] val enabledMetrics = ConcurrentHashMap.newKeySet[MetricsType]()

  def enabled: Set[MetricsType] = enabledMetrics.asScala.toSet

  def isEnabled(metricsType: MetricsType): Boolean = {
    enabledMetrics.contains(metricsType)
  }

  def enable(metricsType: MetricsType): Unit = {
    enabledMetrics.add(metricsType)
  }

  def disable(metricsType: MetricsType): Unit = {
    enabledMetrics.remove(metricsType)
  }
}
