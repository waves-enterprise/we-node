package com.wavesenterprise.api.http

trait UnsignedTxRequest {
  val sender: String
  val password: Option[String]
  val timestamp: Option[Long]
}
