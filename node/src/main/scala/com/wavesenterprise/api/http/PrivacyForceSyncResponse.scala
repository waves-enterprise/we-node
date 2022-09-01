package com.wavesenterprise.api.http

import play.api.libs.json.{Json, OFormat}

case class PrivacyForceSyncResponse(forceRestarted: Int)

object PrivacyForceSyncResponse {
  implicit val privacyForceSyncResponseFormat: OFormat[PrivacyForceSyncResponse] = Json.format
}
