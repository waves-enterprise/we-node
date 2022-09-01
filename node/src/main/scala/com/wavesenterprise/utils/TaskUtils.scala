package com.wavesenterprise.utils

import com.wavesenterprise.crypto.internals.CryptoError
import com.wavesenterprise.transaction.ValidationError
import monix.eval.Task

object TaskUtils {

  @inline
  def deferEitherToTask[L <: ValidationError, R](either: => Either[L, R]): Task[R] =
    Task.defer(eitherToTask(either))

  @inline
  def deferCryptoEitherToTask[L <: CryptoError, R](either: => Either[L, R]): Task[R] =
    Task.defer(cryptoEitherToTask(either))

  @inline
  def eitherToTask[L <: ValidationError, R](either: Either[L, R]): Task[R] =
    Task.fromEither[L, R](error => new RuntimeException(error.toString))(either)

  @inline
  def cryptoEitherToTask[L <: CryptoError, R](either: Either[L, R]): Task[R] =
    Task.fromEither[L, R](error => new RuntimeException(error.toString))(either)
}
