package com.wavesenterprise.api.http

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route}
import com.wavesenterprise.api.http.utils.{HttpLoggingBase, HttpMetricsBase}
import com.wavesenterprise.http.api_key
import com.wavesenterprise.metrics.Metrics.HttpRequestsCacheSettings
import com.wavesenterprise.settings.ApiSettings
import kamon.Kamon
import kamon.metric.TimerMetric

class CompositeHttpService(private val routes: Seq[ApiRoute],
                           private val settings: ApiSettings,
                           protected val httpRequestsCacheSettings: HttpRequestsCacheSettings,
                           val customSwaggerRoute: Option[Route])
    extends HttpLoggingBase
    with HttpMetricsBase {

  override protected val requestProcessingTimer: TimerMetric = Kamon.timer("http.rest.request")

  def withCors: Directive0 =
    if (settings.rest.cors)
      respondWithHeader(`Access-Control-Allow-Origin`.*)
    else pass

  private val headers = List("Authorization", "Content-Type", "X-Requested-With", "Timestamp", api_key.name, "Signature")

  def compositeRoute: Route =
    withCors(routes.map(_.route).reduce(_ ~ _)) ~
      (pathEndOrSingleSlash | path("swagger")) {
        redirect("/api-docs/index.html", StatusCodes.PermanentRedirect)
      } ~
      pathPrefix("api-docs") {
        pathEndOrSingleSlash {
          redirect("/api-docs/index.html", StatusCodes.PermanentRedirect)
        } ~ (path("openapi.json") & get) {
          customSwaggerRoute.getOrElse(getFromResource("opensource-open-api.json"))
        } ~
          getFromResourceDirectory("swagger-ui") ~
          getFromResource("opensource-open-api.json")
      } ~ options {
        respondWithDefaultHeaders(`Access-Control-Allow-Credentials`(true),
                                  `Access-Control-Allow-Headers`(headers),
                                  `Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE))(withCors(complete(StatusCodes.OK)))
      } ~ complete(StatusCodes.NotFound)

  val enrichedCompositeRoute: Route = withLogging(withMeasuring(compositeRoute))

  private def withLogging: Directive0 = extractRequestContext.flatMap { ctx =>
    val start = logRequest(ctx.request)
    mapResponse { response =>
      logResponse(ctx.request, response, start)
    }
  }

  private def withMeasuring: Directive0 = extractRequestContext.flatMap { ctx =>
    val timer = startRequestMeasuring(ctx.request)

    mapResponse { response =>
      timer.stop()
      response
    }
  }
}

object CompositeHttpService {
  def apply(routes: Seq[ApiRoute],
            settings: ApiSettings,
            httpRequestsCacheSettings: HttpRequestsCacheSettings,
            customSwaggerRoute: Option[Route]): CompositeHttpService =
    new CompositeHttpService(routes, settings, httpRequestsCacheSettings, customSwaggerRoute)
}
