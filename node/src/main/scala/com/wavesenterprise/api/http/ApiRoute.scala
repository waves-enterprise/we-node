package com.wavesenterprise.api.http

import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server._
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.{ApiKeyNotValid, HttpEntityTooBig, PrivacyApiKeyNotValid, SignatureError, WrongJson}
import com.wavesenterprise.api.http.auth.ApiProtectionLevel._
import com.wavesenterprise.api.http.auth.AuthRole._
import com.wavesenterprise.api.http.auth._
import com.wavesenterprise.crypto
import com.wavesenterprise.http.{ApiMarshallers, JsonException, api_key}
import com.wavesenterprise.settings.{ApiSettings, AuthorizationSettings}
import monix.execution.Scheduler
import play.api.libs.json._

import java.security.SignatureException
import scala.concurrent.ExecutionContextExecutor

object Response {
  val OK: Map[String, String]                     = Map("response" -> "OK")
  def error(message: String): Map[String, String] = Map("error" -> message)
}

trait ApiRoute extends Directives with CommonApiFunctions with ApiMarshallers with ApiErrorHandler with Oauth2Authorization {

  implicit def schedulerToExecutionContextExecutor(scheduler: Scheduler): ExecutionContextExecutor = {
    ExecutionContexts.fromExecutor(scheduler)
  }

  implicit def routeToStandardRoute(route: Route): StandardRoute = {
    StandardRoute(route)
  }

  def settings: ApiSettings
  def route: Route
  protected def nodeOwner: Address

  protected val generalRejectionHandler: RejectionHandler = RejectionHandler
    .newBuilder()
    .handle {
      case ValidationRejection(_, Some(JsonException(_, errors))) => complete(WrongJson(None, errors))
      case malformed: MalformedRequestContentRejection if malformed.cause.isInstanceOf[akka.http.scaladsl.model.EntityStreamSizeException] =>
        val exception = malformed.cause.asInstanceOf[akka.http.scaladsl.model.EntityStreamSizeException]
        complete(HttpEntityTooBig(exception.actualSize.getOrElse(-1), exception.limit))
    }
    .result()

  protected def json[A: Reads](f: A => ToResponseMarshallable): Route =
    handleExceptions(generalExceptionHandler) {
      handleRejections(generalRejectionHandler) {
        entity(as[A]) { a =>
          complete(f(a))
        }
      }
    }

  protected val generalExceptionHandler: ExceptionHandler = ExceptionHandler {
    case JsResultException(err)    => complete(WrongJson(errors = err))
    case e: NoSuchElementException => complete(WrongJson(Some(e)))
    case e: SignatureException     => complete(SignatureError(e.getMessage))
  }

  def withAuth(protection: ApiProtectionLevel = WithoutProtection, requiredRole: AuthRole = User): Directive0 = {
    settings.auth match {
      case apiKeyAuth: AuthorizationSettings.ApiKey =>
        protection match {
          case WithoutProtection       => pass
          case ApiKeyProtection        => checkApiKeyHeader(apiKeyAuth.apiKeyHashBytes, ApiKeyNotValid)
          case PrivacyApiKeyProtection => checkApiKeyHeader(apiKeyAuth.privacyApiKeyHashBytes, PrivacyApiKeyNotValid)
        }

      case _: AuthorizationSettings.OAuth2 =>
        withOAuth(requiredRole, nodeOwner)

      case _ => pass
    }
  }

  private def checkApiKeyHeader(expectedApiKeyHash: Array[Byte], error: => ApiError): Directive0 =
    optionalHeaderValueByType(api_key).flatMap {
      case Some(k) if crypto.safeIsEqual(crypto.secureHash(k.value), expectedApiKeyHash) => pass
      case _                                                                             => complete(error)
    }

}
