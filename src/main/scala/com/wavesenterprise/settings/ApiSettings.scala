package com.wavesenterprise.settings

import cats.Show
import cats.implicits.showInterpolator
import com.wavesenterprise.settings.api.GrpcApiSettings
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class ApiSettings(rest: RestApiSettings, grpc: GrpcApiSettings, auth: AuthorizationSettings)

object ApiSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[ApiSettings] = deriveReader

  implicit val toPrintable: Show[ApiSettings] = { x =>
    import x._

    s"""
       |restSettings:
       |  ${show"$rest".replace("\n", "\n--")}
       |grpcSettings:
       |  ${show"$grpc".replace("\n", "\n--")}
       |authorizationSettings:
       |  ${show"$auth".replace("\n", "\n--")}
     """.stripMargin
  }
}
