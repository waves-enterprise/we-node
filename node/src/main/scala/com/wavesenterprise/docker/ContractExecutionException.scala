package com.wavesenterprise.docker

import com.wavesenterprise.transaction.ValidationError

/**
  * Contract execution exception
  *
  * @param message exception message
  */
class ContractExecutionException(message: String, cause: Throwable, val code: Option[Int] = None) extends RuntimeException(message, cause) {

  def this(message: String, code: Option[Int]) {
    this(message, null, code)
  }

  def this(message: String) {
    this(message, None)
  }
}

object ContractExecutionException {

  def apply(tuple: (Throwable, String)): ContractExecutionException = new ContractExecutionException(tuple._2, tuple._1)

  def apply(error: ValidationError): ContractExecutionException = new ContractExecutionException(error.toString)
}
