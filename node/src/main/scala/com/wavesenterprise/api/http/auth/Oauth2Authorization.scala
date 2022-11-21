package com.wavesenterprise.api.http.auth

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.BasicDirectives.provide
import akka.http.scaladsl.server.{Directive0, Directive1}
import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.{
  AuthTokenExpired,
  CantParseJwtClaims,
  InvalidNodeOwnerAddress,
  InvalidTokenError,
  MissingAuthorizationMetadata,
  NoRequiredAuthRole
}
import com.wavesenterprise.api.http._
import com.wavesenterprise.api.http.auth.AuthRole.{Administrator, PrivacyUser}
import com.wavesenterprise.settings.{ApiSettings, AuthorizationSettings}
import com.wavesenterprise.utils.{ScorexLogging, Time}
import pdi.jwt.{JwtAlgorithm, JwtClaim, JwtJson, JwtOptions}
import play.api.libs.json.Json

import scala.util.{Success, _}

trait Oauth2Authorization extends ScorexLogging {
  def settings: ApiSettings
  def time: Time

  type ErrorOr[Data] = Either[ApiError, Data]

  def withOAuth(requiredRole: AuthRole, nodeOwner: Address): Directive0 = {
    extractJwtContent.flatMap {
      case Right(content) =>
        (checkRole(content.roles, requiredRole) >>
          (requiredRole match {
            case Administrator | PrivacyUser => checkNodeOwner(content.addresses, nodeOwner)
            case _                           => Right(())
          }))
          .map(_ => pass)
          .valueOr(complete(_))
      case Left(error) =>
        complete(error)
    }
  }

  private val extractOAuthToken: Directive1[ErrorOr[OAuth2BearerToken]] =
    extractCredentials.flatMap {
      case Some(token: OAuth2BearerToken) => provide(Right(token))
      case _                              => provide(Left(MissingAuthorizationMetadata))
    }

  private val extractJwtClaims: Directive1[ErrorOr[JwtClaim]] =
    extractOAuthToken.flatMap {
      case Right(OAuth2BearerToken(token)) => provide(validateJwtToken(token))
      case Left(error)                     => provide(Left(error))
    }

  private val extractJwtContent: Directive1[ErrorOr[JwtContent]] =
    extractJwtClaims.flatMap {
      case Right(claims) =>
        extractJwtContent(claims) match {
          case Right(content) => provide(Right(content))
          case Left(error)    => provide(Left(error))
        }
      case Left(error) => provide(Left(error))
    }

  private def validateJwtToken(token: String): ErrorOr[JwtClaim] = {
    settings.auth match {
      case AuthorizationSettings.OAuth2(publicKey) =>
        val validatingOptions = JwtOptions(signature = true, expiration = false, notBefore = false)
        val decodingResult    = JwtJson.decode(token, publicKey, Seq(JwtAlgorithm.RS256), validatingOptions)
        decodingResult match {
          case Success(claim) =>
            validateTokenTiming(claim)
          case Failure(ex) =>
            log.error("Failed to decode JWT", ex)
            Left(InvalidTokenError)
        }

      case _ =>
        Left(InvalidTokenError)
    }
  }

  private def extractJwtContent(jwtClaim: JwtClaim): ErrorOr[JwtContent] = {
    Either
      .catchNonFatal(Json.parse(jwtClaim.content).as[JwtContent])
      .leftMap { ex =>
        log.error(s"Failed to parse JWT claims", ex)
        CantParseJwtClaims
      }
  }

  private def isRolesAllowed(providedRoles: Set[AuthRole], requiredRole: AuthRole): Boolean = {
    if (requiredRole == AuthRole.User)
      providedRoles.intersect(AuthRole.values.toSet).nonEmpty
    else
      providedRoles.contains(requiredRole)
  }

  private def checkRole(providedRoles: Set[AuthRole], requiredRole: AuthRole): Either[ApiError, Unit] =
    Either.cond(
      isRolesAllowed(providedRoles, requiredRole),
      (),
      NoRequiredAuthRole(requiredRole)
    )

  private def checkNodeOwner(providedAddresses: Set[AddressEntry], nodeOwner: Address): Either[ApiError, Unit] = {
    def filteredProvidedAddresses = providedAddresses.collect {
      case AddressEntry(address, AddressEntry.NodeType) => address
    }

    Either.cond(
      filteredProvidedAddresses.contains(nodeOwner),
      (),
      InvalidNodeOwnerAddress
    )
  }

  private def validateTokenTiming(jwtClaim: JwtClaim): ErrorOr[JwtClaim] = {
    val end = jwtClaim.expiration
    Either.cond(end.isDefined && time.correctedTime() <= end.get * 1000, jwtClaim, AuthTokenExpired)
  }
}
