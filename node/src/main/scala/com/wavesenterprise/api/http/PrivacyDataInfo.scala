package com.wavesenterprise.api.http

import play.api.libs.json.{Json, OFormat}

case class PrivacyDataInfo(filename: String, size: Int, timestamp: Long, author: String, comment: String)

object PrivacyDataInfo {
  val fileNameMaxLength: Int = 32768
  val authorMaxLength: Int   = 32768
  val commentMaxLength: Int  = 32768

  implicit val connectFormat: OFormat[PrivacyDataInfo] = Json.format
}
