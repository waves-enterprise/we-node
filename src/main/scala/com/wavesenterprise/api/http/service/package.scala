package com.wavesenterprise.api.http

import com.wavesenterprise.api.http.ApiError.OverflowError

import scala.util.{Failure, Success, Try}

package object service {
  def validateSum(x: Int, y: Int): Either[ApiError, Int] = {
    Try(Math.addExact(x, y)) match {
      case Failure(_)     => Left(OverflowError)
      case Success(value) => Right(value)
    }
  }
}
