package com.wavesenterprise.api.grpc

import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.wavesenterprise.api.http.utils.{HttpLoggingBase, HttpMetricsBase}
import com.wavesenterprise.metrics.Metrics.HttpRequestsCacheSettings
import kamon.Kamon
import kamon.metric.TimerMetric

import scala.concurrent.{ExecutionContext, Future}

class CompositeGrpcService(protected val httpRequestsCacheSettings: HttpRequestsCacheSettings,
                           private val handlers: PartialFunction[HttpRequest, Future[HttpResponse]]*)(implicit ec: ExecutionContext)
    extends HttpLoggingBase
    with HttpMetricsBase {

  override protected val requestProcessingTimer: TimerMetric = Kamon.timer("http.grpc.request")

  val compositeHandler: HttpRequest => Future[HttpResponse] = ServiceHandler.concatOrNotFound(handlers: _*)

  def enrichedCompositeHandler: HttpRequest => Future[HttpResponse] = withLogging(withMeasuring(compositeHandler))

  private def withLogging(handler: HttpRequest => Future[HttpResponse]): HttpRequest => Future[HttpResponse] = { request =>
    val start = logRequest(request)
    handler(request).map { response =>
      logResponse(request, response, start)
    }
  }

  private def withMeasuring(handler: HttpRequest => Future[HttpResponse]): HttpRequest => Future[HttpResponse] = { request =>
    val timer = startRequestMeasuring(request)

    handler(request).map { response =>
      timer.stop()
      response
    }
  }
}

object CompositeGrpcService {
  def apply(httpRequestsCacheSettings: HttpRequestsCacheSettings, handlers: PartialFunction[HttpRequest, Future[HttpResponse]]*)(
      implicit ec: ExecutionContext): CompositeGrpcService =
    new CompositeGrpcService(httpRequestsCacheSettings, handlers: _*)(ec)
}
