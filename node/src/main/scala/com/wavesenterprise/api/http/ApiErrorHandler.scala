package com.wavesenterprise.api.http

import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{complete, extractRequest, extractUri}
import akka.http.scaladsl.server.ExceptionHandler
import com.wavesenterprise.utils.ScorexLogging
import play.api.libs.json.Json

trait ApiErrorHandler extends ScorexLogging {

  import ApiErrorHandler._

  implicit val exceptionHandler: ExceptionHandler = ExceptionHandler {
    case ex: Throwable =>
      extractUri { uri =>
        extractRequest { request =>
          log.error(s"Exception occurred on API call to '$uri', request: '$request'", ex)
          complete(
            HttpResponse(InternalServerError,
                         entity = HttpEntity(ContentTypes.`application/json`, Json.toJson(Response.error(InternalServerErrorMessage)).toString()))
          )
        }
      }
  }

}

object ApiErrorHandler {

  val InternalServerErrorMessage = "There was an internal server error."
}
