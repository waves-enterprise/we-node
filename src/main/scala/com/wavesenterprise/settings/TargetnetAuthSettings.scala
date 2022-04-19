package com.wavesenterprise.settings

import cats.Show
import com.wavesenterprise.api.http.auth.AuthType
import pureconfig.error.{CannotConvert, ConfigReaderFailures}
import pureconfig.generic.semiauto._
import pureconfig.{ConfigObjectCursor, ConfigReader}

import scala.concurrent.duration.FiniteDuration

sealed trait TargetnetAuthSettings

object TargetnetAuthSettings extends WEConfigReaders {

  case object NoAuth extends TargetnetAuthSettings

  case class Oauth2(authorizationToken: String, authorizationServiceUrl: String, tokenUpdateInterval: FiniteDuration) extends TargetnetAuthSettings

  object Oauth2 extends WEConfigReaders {
    implicit val configReader: ConfigReader[Oauth2] = deriveReader
  }

  implicit val toPrintable: Show[TargetnetAuthSettings] = {
    case NoAuth =>
      s"""
         |type: no-auth
         """.stripMargin
    case Oauth2(_, authorizationServiceUrl, tokenUpdateInterval) =>
      s"""
         |type: ${AuthType.Oauth2}
         |auth-service-url: $authorizationServiceUrl
         |token-update-interval: $tokenUpdateInterval
         """.stripMargin
  }

  private def extractByType(tpe: String, objectCursor: ConfigObjectCursor): Either[ConfigReaderFailures, TargetnetAuthSettings] = {
    tpe match {
      case AuthType.ApiKey.value => Right(TargetnetAuthSettings.NoAuth)
      case AuthType.Oauth2.value => Oauth2.configReader.from(objectCursor)
      case unknown =>
        objectCursor.failed(
          CannotConvert(objectCursor.value.toString,
                        "TargetnetAuthSettings",
                        s"type has value '$unknown' instead of '${AuthType.ApiKey}' or '${AuthType.Oauth2}'"))
    }
  }

  implicit val configReader: ConfigReader[TargetnetAuthSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      typeCursor   <- objectCursor.atKey("type")
      tpe          <- typeCursor.asString
      settings     <- extractByType(tpe, objectCursor)
    } yield settings
  }
}
