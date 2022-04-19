package com.wavesenterprise.docker

import com.wavesenterprise.state.DataEntry
import play.api.libs.json.{Json, OFormat}

/**
  * Result of docker contract execution
  */
sealed trait ContractExecution

/**
  * Result of contract execution that returned some contract error
  */
case class ContractExecutionError(code: Int, message: String) extends ContractExecution

/**
  * Successful result of contract execution
  */
case class ContractExecutionSuccess(results: List[DataEntry[_]]) extends ContractExecution

object ContractExecutionSuccess {

  implicit val format: OFormat[ContractExecutionSuccess] = Json.format
}

object ContractExecutionError {
  val FatalErrorCode: Int       = 0
  val RecoverableErrorCode: Int = 1
}

case object ContractUpdateSuccess extends ContractExecution
