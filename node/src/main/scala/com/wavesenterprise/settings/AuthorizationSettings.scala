package com.wavesenterprise.settings

import java.security.spec.X509EncodedKeySpec
import java.security.{KeyFactory, PublicKey}

import cats.Show
import cats.syntax.either._
import com.wavesenterprise.api.http.auth.AuthType
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.utils.{Base58, Base64}
import pureconfig.error.{CannotConvert, ConfigReaderFailures}
import pureconfig.generic.semiauto._
import pureconfig.{ConfigObjectCursor, ConfigReader}

sealed trait AuthorizationSettings

object AuthorizationSettings {

  case class OAuth2(publicKey: String) extends AuthorizationSettings {

    val authServicePublicKey: PublicKey = {
      val publicBytes = Base64
        .decode(publicKey)
        .toEither
        .leftMap(_ => GenericError(s"Failed to decode authorization public key: '$publicKey'"))
        .explicitGet()

      val keySpec    = new X509EncodedKeySpec(publicBytes)
      val keyFactory = KeyFactory.getInstance("RSA")

      keyFactory.generatePublic(keySpec)
    }
  }

  object OAuth2 extends WEConfigReaders {
    implicit val configReader: ConfigReader[OAuth2] = deriveReader
  }

  case class ApiKey(apiKeyHash: String, privacyApiKeyHash: String) extends AuthorizationSettings {
    require(apiKeyHash.nonEmpty, ApiKey.API_KEY_EMPTY_MESSAGE)
    require(privacyApiKeyHash.nonEmpty, ApiKey.PRIVACY_API_KEY_EMPTY_MESSAGE)

    val apiKeyHashBytes: Array[Byte] = Base58
      .decode(apiKeyHash)
      .toEither
      .leftMap(_ => GenericError(s"Failed to decode api-key-hash: '$apiKeyHash'"))
      .explicitGet()

    val privacyApiKeyHashBytes: Array[Byte] = Base58
      .decode(privacyApiKeyHash)
      .toEither
      .leftMap(_ => GenericError(s"Failed to decode privacy-api-key-hash: '$privacyApiKeyHash'"))
      .explicitGet()
  }

  object ApiKey extends WEConfigReaders {
    private[settings] val API_KEY_EMPTY_MESSAGE         = "'node.api.auth.api-key-hash' setting is empty"
    private[settings] val PRIVACY_API_KEY_EMPTY_MESSAGE = "'node.api.auth.privacy-api-key-hash' setting is empty"

    implicit val configReader: ConfigReader[ApiKey] = deriveReader
  }

  private def extractByType(tpe: String, objectCursor: ConfigObjectCursor): Either[ConfigReaderFailures, AuthorizationSettings] = {
    tpe.toLowerCase match {
      case AuthType.Oauth2.value => OAuth2.configReader.from(objectCursor)
      case AuthType.ApiKey.value => ApiKey.configReader.from(objectCursor)
      case unknown =>
        objectCursor.failed(
          CannotConvert(objectCursor.value.toString,
                        "AuthorizationSettings",
                        s"type has value '$unknown' instead of '${AuthType.Oauth2}' or '${AuthType.ApiKey}'"))
    }
  }

  implicit val configReader: ConfigReader[AuthorizationSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      typeCursor   <- objectCursor.atKey("type")
      tpe          <- typeCursor.asString
      settings     <- extractByType(tpe, objectCursor)
    } yield settings
  }

  implicit val toPrintable: Show[AuthorizationSettings] = {
    case AuthorizationSettings.ApiKey(apiKeyHashBase58, privacyApiKeyHashBase58) =>
      s"""
          |apiKeyHash: $apiKeyHashBase58
          |privacyApiKeyHash: $privacyApiKeyHashBase58
      |""".stripMargin

    case AuthorizationSettings.OAuth2(pubKeyBase64) =>
      s"""
         |public-key: "$pubKeyBase64"
         |""".stripMargin
  }
}
