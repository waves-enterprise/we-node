package com.wavesenterprise.api.http.utils

import akka.http.scaladsl.model.HttpRequest
import com.google.common.cache.{CacheBuilder, RemovalNotification}
import com.wavesenterprise.metrics.Metrics.HttpRequestsCacheSettings
import kamon.metric.{StartedTimer, Timer, TimerMetric}

import java.util.concurrent.TimeUnit

trait HttpMetricsBase {

  protected def requestProcessingTimer: TimerMetric

  protected def httpRequestsCacheSettings: HttpRequestsCacheSettings

  private val timerCache = CacheBuilder
    .newBuilder()
    .maximumSize(httpRequestsCacheSettings.maxSize)
    .expireAfterWrite(httpRequestsCacheSettings.expireAfter.toMillis, TimeUnit.MILLISECONDS)
    .removalListener((notification: RemovalNotification[(String, String), Timer]) => {
      val (path, method) = notification.getKey
      requestProcessingTimer.remove(buildTags(path, method))
    })
    .build[(String, String), Timer]

  protected def startRequestMeasuring(request: HttpRequest): StartedTimer = {
    val path   = request.uri.path.toString
    val method = request.method.name

    val timer = timerCache.get(path -> method,
                               { () =>
                                 requestProcessingTimer
                                   .refine(buildTags(path, method))
                               })

    timer.start()
  }

  @inline
  private def buildTags(path: String, method: String): Map[String, String] =
    Map("path" -> path, "method" -> method)
}
