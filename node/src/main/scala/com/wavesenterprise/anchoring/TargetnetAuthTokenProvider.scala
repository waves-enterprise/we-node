package com.wavesenterprise.anchoring

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import com.wavesenterprise.authorization.{AuthServiceWorker, AuthToken}
import com.wavesenterprise.settings.TargetnetAuthSettings
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.Cancelable
import monix.reactive.Observable

import scala.collection.immutable.Seq
import scala.concurrent.duration._

trait TargetnetAuthTokenProvider {
  def start(): Cancelable
  def authHeaders: Seq[HttpHeader]
  def isAlive: Boolean
}

object TargetnetAuthTokenProvider {

  def apply(authType: TargetnetAuthSettings)(implicit
      system: ActorSystem,
      materializer: Materializer,
      scheduler: monix.execution.Scheduler): TargetnetAuthTokenProvider = authType match {
    case TargetnetAuthSettings.NoAuth                        => NoToken
    case authService @ TargetnetAuthSettings.Oauth2(_, _, _) => new AuthServiceToken(authService)(system, materializer, scheduler)
  }

  case object NoToken extends TargetnetAuthTokenProvider {
    override val authHeaders: Seq[HttpHeader] = Seq.empty

    override def start(): Cancelable = Cancelable.empty

    override def isAlive: Boolean = true
  }

  case class TargetnetAuthorizationConfig(
      targetnetAuthServiceUrl: String,
      tokenUpdateDelay: FiniteDuration
  )

  class AuthServiceToken(authService: TargetnetAuthSettings.Oauth2)(implicit
      system: ActorSystem,
      materializer: Materializer,
      scheduler: monix.execution.Scheduler)
      extends TargetnetAuthTokenProvider
      with ScorexLogging {

    @volatile private var currentToken: AuthToken = AuthToken("uninitialized", "uninitialized")
    @volatile private var isActualToken: Boolean  = false
    override def authHeaders: Seq[HttpHeader]     = Seq(Authorization(OAuth2BearerToken(currentToken.accessToken)))

    private val delayBetweenErrors = 10 seconds
    private val httpClient         = Http()

    private val tokenUpdateStream: Observable[AuthToken] = Observable
      .interval(authService.tokenUpdateInterval)
      .mapEval(_ => AuthServiceWorker.getAccessToken(httpClient, authService.authorizationServiceUrl, authService.authorizationToken))
      .map(_.fold(throw _, identity))
      .doOnError { ex =>
        isActualToken = false
        log.error(s"Failed to authorize on targetnet auth service '${authService.authorizationServiceUrl}'", ex)
        Task.raiseError(ex).delayExecution(delayBetweenErrors)
      }
      .onErrorRestartUnlimited
      .doOnNext { token =>
        Task {
          log.info(s"Authorize token successfully updated from '${authService.authorizationServiceUrl}'")
          currentToken = token
          isActualToken = true
        }
      }

    override def start(): Cancelable = {
      log.debug(s"Starting for targetnet auth service url '${authService.authorizationServiceUrl}'...")
      tokenUpdateStream.subscribe()
    }

    override def isAlive: Boolean = isActualToken
  }
}
