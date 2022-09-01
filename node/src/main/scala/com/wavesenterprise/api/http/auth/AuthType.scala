package com.wavesenterprise.api.http.auth

import enumeratum.values.StringEnumEntry
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

sealed abstract class AuthType(val value: String) extends StringEnumEntry

object AuthType {
  case object ApiKey extends AuthType("api-key")
  case object Oauth2 extends AuthType("oauth2")

  implicit val configReader: ConfigReader[AuthType] = deriveEnumerationReader[AuthType]
}
