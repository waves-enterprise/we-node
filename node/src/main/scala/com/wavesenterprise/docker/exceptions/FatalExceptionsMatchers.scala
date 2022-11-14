package com.wavesenterprise.docker.exceptions

import com.github.dockerjava.api.exception.DockerException
import com.wavesenterprise.docker.CircuitBreakerSupport.CircuitBreakerError
import com.wavesenterprise.docker.ContractExecutionException
import com.wavesenterprise.docker.DockerEngine.ImageDigestValidationException

object FatalExceptionsMatchers {
  type ExceptionsMatcher = Throwable => Boolean

  val emptyMatcher: ExceptionsMatcher = (_) => false

  val prepareExecutionExceptionsMatcher: ExceptionsMatcher = {
    case _: ImageDigestValidationException => true
    //when image not found
    case err: ContractExecutionException if Option(err.getCause).exists(_.getMessage.matches(".*unknown.+not found.*")) => true
    // unknown docker error
    case ContractExecutionException(_, _: DockerException, _) => true
    case _                                                    => false
  }

  val executionExceptionsMatcher: ExceptionsMatcher = {
    // unknown docker error
    case ContractExecutionException(_, _: DockerException, _) => true
    case _                                                    => false
  }

  val allFatalExceptionsMatcher: ExceptionsMatcher = {
    case _: CircuitBreakerError                                                           => true
    case err if prepareExecutionExceptionsMatcher(err) || executionExceptionsMatcher(err) => true
    case _                                                                                => false
  }

  def fatalTxExceptionToString(fatal: Throwable, stringTx: String): String = fatal match {
    case err: CircuitBreakerError =>
      s"Contract circuit breaker error: ${err.message} for transaction '$stringTx'"
    case err: ImageDigestValidationException =>
      s"Contract image validation error: $err for transaction '$stringTx'"
    case err: ContractExecutionException if Option(err.getCause).exists(_.getMessage.matches(".*unknown.+not found.*")) =>
      s"Contract execution error: $err for transaction '$stringTx'"
    case err: ContractExecutionException if Option(err.getCause).exists(_.isInstanceOf[DockerException]) =>
      s"Contract docker error: $err for transaction '$stringTx'"
  }

}
