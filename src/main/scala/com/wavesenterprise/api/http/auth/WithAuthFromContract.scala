package com.wavesenterprise.api.http.auth

import akka.http.scaladsl.model.headers.{ModeledCustomHeader, ModeledCustomHeaderCompanion}
import akka.http.scaladsl.server.{Directive0, Directive1, Directives}
import com.wavesenterprise.api.http.ApiError.InvalidTokenError
import com.wavesenterprise.docker.ContractAuthTokenService
import com.wavesenterprise.docker.ContractExecutor.ContractTxClaimContent

import scala.util.Try

object `X-Contract-Api-Token` extends ModeledCustomHeaderCompanion[`X-Contract-Api-Token`] {

  override val name = "X-Contract-Api-Token"

  override def parse(value: String): Try[`X-Contract-Api-Token`] = Try(new `X-Contract-Api-Token`(value))
}

final class `X-Contract-Api-Token`(override val value: String) extends ModeledCustomHeader[`X-Contract-Api-Token`] {

  override def companion: `X-Contract-Api-Token`.type = `X-Contract-Api-Token`

  override def renderInRequests: Boolean = true

  override def renderInResponses: Boolean = false
}

trait WithAuthFromContract extends Directives {

  def contractAuthTokenService: Option[ContractAuthTokenService]

  def withContractAuth: Directive0 = withContractAuthClaim.map(_ => ())

  def withContractAuthClaim: Directive1[ContractTxClaimContent] = {
    contractAuthTokenService match {
      case Some(service) =>
        optionalHeaderValueByType[`X-Contract-Api-Token`](()).flatMap {
          case Some(header) =>
            service
              .validate(header.value)(ContractTxClaimContent.format)
              .map(provide)
              .getOrElse(complete(InvalidTokenError))
          case _ => reject
        }
      case None => reject
    }
  }
}
