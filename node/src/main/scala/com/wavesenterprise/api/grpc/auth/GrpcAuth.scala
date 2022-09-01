package com.wavesenterprise.api.grpc.auth

import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import cats.syntax.either.{catsSyntaxEither, catsSyntaxEitherObject}
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.{
  ApiKeyNotValid,
  AuthTokenExpired,
  CantParseJwtClaims,
  InvalidNodeOwnerAddress,
  InvalidTokenError,
  MissingAuthorizationMetadata,
  NoRequiredAuthRole,
  PrivacyApiKeyNotValid
}
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.{ApiKeyProtection, PrivacyApiKeyProtection, WithoutProtection}
import com.wavesenterprise.api.http.auth.AuthRole.User
import com.wavesenterprise.api.http.auth.{AddressEntry, ApiProtectionLevel, AuthRole, JwtContent}
import com.wavesenterprise.crypto
import com.wavesenterprise.http.api_key
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.utils.{ScorexLogging, Time}
import monix.eval.Task
import pdi.jwt.{JwtAlgorithm, JwtClaim, JwtJson, JwtOptions}
import play.api.libs.json.Json

trait GrpcAuth extends ScorexLogging {
  def authSettings: AuthorizationSettings
  def nodeOwner: Address
  def time: Time

  private val apiKeyMetaHeader = api_key.name
  private val oAuthMetaHeader  = "Authorization"

  def withAuth[T](metadata: Metadata, protection: ApiProtectionLevel = WithoutProtection, requiredRole: AuthRole = User)(
      f: => T): Either[GrpcServiceException, T] =
    withAuthEvent(metadata, protection, requiredRole)(f)
      .leftMap(_.asGrpcServiceException)

  def withAuthTask[T](metadata: Metadata, protection: ApiProtectionLevel = WithoutProtection, requiredRole: AuthRole = User)(t: Task[T]): Task[T] = {
    Task.fromEither {
      doAuth(metadata, protection, requiredRole)
        .leftMap(_.asGrpcServiceException)
    } >> Task.defer(t)
  }

  def withAuthEvent[T](metadata: Metadata, protection: ApiProtectionLevel = WithoutProtection, requiredRole: AuthRole = User)(
      f: => T = ()): Either[ApiError, T] = {
    doAuth(metadata, protection, requiredRole).map(_ => f)
  }

  private def doAuth(metadata: Metadata, protection: ApiProtectionLevel, requiredRole: AuthRole): Either[ApiError, Unit] = {
    authSettings match {
      case apiKeyAuth: AuthorizationSettings.ApiKey =>
        protection match {
          case WithoutProtection       => Right(())
          case ApiKeyProtection        => withApiKeyAuth(metadata, apiKeyAuth.apiKeyHashBytes, ApiKeyNotValid)
          case PrivacyApiKeyProtection => withApiKeyAuth(metadata, apiKeyAuth.privacyApiKeyHashBytes, PrivacyApiKeyNotValid)
          case unsupportedProtection   => Left(ApiError.UnimplementedError(s"'$unsupportedProtection' protection is not supported for gRPC yet"))
        }

      case AuthorizationSettings.OAuth2(publicKey) => withOAuth(metadata, nodeOwner, publicKey, requiredRole)
      case _                                       => Left(ApiError.UnimplementedError(s"Unsupported authorization settings for gRPC"))
    }
  }

  private def withApiKeyAuth(metadata: Metadata, expectedApiKeyHash: Array[Byte], error: ApiError): Either[ApiError, Unit] = {
    for {
      authToken <- getHeaderFromMeta(metadata, apiKeyMetaHeader).toRight(MissingAuthorizationMetadata)
      _ <- Either.cond(
        crypto.safeIsEqual(crypto.secureHash(authToken), expectedApiKeyHash),
        (),
        error
      )
    } yield ()
  }

  /**
    * Some gRPC clients transforms metadata keys for some reason.
    */
  private def getHeaderFromMeta(metadata: Metadata, headerName: String): Option[String] =
    metadata
      .getText(headerName)
      .orElse(metadata.getText(headerName.toLowerCase))
      .orElse(metadata.getText(headerName.toUpperCase))

  def parseAuthToken(typedToken: String): Either[ApiError, String] = {
    Either
      .catchNonFatal {
        val pattern                   = "(\\w+ )?(.+)".r
        val pattern(tokenType, token) = typedToken
        Option(tokenType) -> token
      }
      .leftMap { ex =>
        log.error("gRPC OAuth token parsing failure", ex)
        InvalidTokenError
      }
      .flatMap {
        case (None, token) =>
          log.info("Empty gRPC OAuth token type. Using 'Bearer'")
          Right(token)
        case (Some(tokenType), token) =>
          Either.cond(
            tokenType.init.toLowerCase == "bearer",
            token, {
              log.warn(s"Unsupported token type: '${tokenType.init}'")
              InvalidTokenError
            }
          )
      }
  }

  def withOAuth(metadata: Metadata, nodeOwner: Address, key: String, requiredRole: AuthRole): Either[ApiError, Unit] =
    for {
      authHeader <- getHeaderFromMeta(metadata, oAuthMetaHeader).toRight(MissingAuthorizationMetadata)
      token      <- parseAuthToken(authHeader)
      claim      <- decodeJwt(token, key)
      validClaim <- validateTokenTiming(claim)
      jwtContent <- extractJwtContent(validClaim)
      _          <- checkRole(jwtContent.roles, requiredRole)
      _          <- checkNodeOwner(requiredRole, jwtContent.addresses, nodeOwner)
    } yield ()

  private def decodeJwt(token: String, key: String): Either[ApiError, JwtClaim] = {
    val validatingOptions = JwtOptions(signature = true, expiration = false, notBefore = false)
    JwtJson.decode(token, key, Seq(JwtAlgorithm.RS256), validatingOptions).toEither.leftMap { ex =>
      log.error("Failed to decode JWT", ex)
      InvalidTokenError
    }
  }

  private def validateTokenTiming(jwtClaim: JwtClaim): Either[ApiError, JwtClaim] = {
    val end = jwtClaim.expiration
    Either.cond(end.isDefined && time.correctedTime() <= end.get * 1000, jwtClaim, AuthTokenExpired)
  }

  private def extractJwtContent(jwtClaim: JwtClaim): Either[ApiError, JwtContent] = {
    Either
      .catchNonFatal(Json.parse(jwtClaim.content).as[JwtContent])
      .leftMap { ex =>
        log.error(s"Failed to parse JWT claims", ex)
        CantParseJwtClaims
      }
  }

  private def checkRole(providedRoles: Set[AuthRole], requiredRole: AuthRole): Either[ApiError, Unit] =
    Either.cond(
      isRolesAllowed(providedRoles, requiredRole),
      (),
      NoRequiredAuthRole(requiredRole)
    )

  private def isRolesAllowed(providedRoles: Set[AuthRole], requiredRole: AuthRole): Boolean = {
    if (requiredRole == AuthRole.User)
      providedRoles.intersect(AuthRole.values.toSet).nonEmpty
    else
      providedRoles.contains(requiredRole)
  }

  private def checkNodeOwner(requiredRole: AuthRole, providedAddresses: Set[AddressEntry], nodeOwner: Address): Either[ApiError, Unit] = {
    requiredRole match {
      case AuthRole.Administrator | AuthRole.PrivacyUser =>
        val filteredProvidedAddresses = providedAddresses.collect {
          case AddressEntry(address, AddressEntry.NodeType) => address
        }

        Either.cond(
          filteredProvidedAddresses.contains(nodeOwner),
          (),
          InvalidNodeOwnerAddress
        )
      case _ => Right(())
    }
  }
}
