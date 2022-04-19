package com.wavesenterprise.authorization

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, Location, OAuth2BearerToken}
import akka.stream.Materializer
import akka.util.ByteString
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import play.api.libs.json.Json.parse

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class ExternalAuthorizationException(cause: Throwable)
    extends RuntimeException("Error occurred during request to external authorization service, " +
                               "probably 'auth-service-url' is not correct or service is not available",
                             cause)

/**
  * Provides OAuth2 token
  * Repeatedly calls auth service to get a fresh token
  */
object AuthServiceWorker extends ScorexLogging {

  private case class HttpResponseWithBody(response: HttpResponse, body: String)

  def getAccessToken(httpClient: HttpExt, authServiceUrl: String, authServiceToken: String)(
      implicit materializer: Materializer,
      exc: ExecutionContext): Task[Either[ExternalAuthorizationException, AuthToken]] = Task.defer {
    val request = HttpRequest(
      uri = Uri(authServiceUrl),
      method = HttpMethods.POST
    ).withHeaders(Authorization(OAuth2BearerToken(authServiceToken)))

    sendRequest(httpClient, request)(materializer, exc).flatMap { response =>
      Task.deferFuture(responseBody(response)).map { body =>
        parseAuthResponse(HttpResponseWithBody(response, body))
      }
    }
  }

  private def sendRequest(httpClient: HttpExt, request: HttpRequest)(implicit materializer: Materializer, exc: ExecutionContext): Task[HttpResponse] =
    Task.defer {
      log.info(s"Sending authorization request to '${request.uri}'")

      Task.deferFuture(httpClient.singleRequest(request)).flatMap { response =>
        response.status match {
          case StatusCodes.PermanentRedirect | StatusCodes.TemporaryRedirect =>
            response.discardEntityBytes()
            response
              .header[Location]
              .map { location =>
                val oldHostname = request.uri.authority.host.address
                val newHostname = location.uri.authority.host.address
                if (oldHostname == newHostname) {
                  val newRequest = request.withUri(location.uri)
                  Task.deferFuture(httpClient.singleRequest(newRequest))
                } else {
                  Task.raiseError(
                    new RuntimeException(s"Initial request hostname '$oldHostname' is not equal to redirection hostname '$newHostname'"))
                }
              }
              .getOrElse(Task.raiseError(
                new RuntimeException(s"Location header is not found for response with status '${response.status}' for ${request.uri}")))
          case _ => Task.eval(response)
        }
      }
    }

  private def parseAuthResponse(httpResponseWithBody: HttpResponseWithBody): Either[ExternalAuthorizationException, AuthToken] = {
    Try {
      parse(httpResponseWithBody.body).as[AuthToken]
    }.toEither.left.map(ExternalAuthorizationException)
  }

  private def responseBody(response: HttpResponse)(implicit materializer: Materializer, exc: ExecutionContext): Future[String] = {
    response.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
  }
}
