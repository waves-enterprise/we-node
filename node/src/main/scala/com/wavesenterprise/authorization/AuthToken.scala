package com.wavesenterprise.authorization

import play.api.libs.json.JsonConfiguration.Aux
import play.api.libs.json.JsonNaming.SnakeCase
import play.api.libs.json.{Json, JsonConfiguration, OFormat}

case class AuthToken(
    accessToken: String,
    refreshToken: String
)
object AuthToken {
  implicit val config: Aux[Json.MacroOptions] = JsonConfiguration(SnakeCase)
  implicit val format: OFormat[AuthToken]     = Json.format
}
