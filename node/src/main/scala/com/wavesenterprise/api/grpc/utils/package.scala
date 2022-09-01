package com.wavesenterprise.api.grpc

import akka.grpc.scaladsl.MetadataBuilder
import akka.grpc.{GrpcServiceException, scaladsl}
import akka.http.scaladsl.model.StatusCodes
import cats.implicits._
import com.wavesenterprise.api.http.{ApiError, ApiErrorBase}
import com.wavesenterprise.crypto.internals.CryptoError
import com.wavesenterprise.docker.ContractExecutionException
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError
import io.grpc.Status
import monix.eval.Task

package object utils {

  val ErrorCodeMetadataKey    = "error-code"
  val ErrorMessageMetadataKey = "error-message"
  val ConnectionIdMetadataKey = "connection-id"
  val ConnectionIdMaxLength   = 16

  def parseTxId(txId: String): Either[GrpcServiceException, ByteStr] = {
    ByteStr
      .decodeBase58(txId)
      .toEither
      .leftMap(ex => ValidationError.GenericError(s"Cannot parse transaction id '$txId': $ex").asGrpcServiceException)
  }

  implicit class EitherApiErrorExt[T, E <: ApiErrorBase](val either: Either[E, T]) extends AnyVal {
    def asTask: Task[T] = Task.fromTry(either.left.map(_.asGrpcServiceException).toTry)
  }

  implicit class ContractExecutionExceptionExt(cee: ContractExecutionException) {
    def asGrpcServiceException(status: Status): GrpcServiceException = {
      new GrpcServiceException(status.withDescription(cee.getMessage))
    }
  }

  implicit class ValidationErrorExt(private val validationError: ValidationError) extends AnyVal {
    def asGrpcServiceException: GrpcServiceException = {
      ApiError.fromValidationError(validationError).asGrpcServiceException
    }
  }

  implicit class CryptoErrorExt(private val cryptoError: CryptoError) extends AnyVal {
    def asGrpcServiceException: GrpcServiceException = {
      ApiError.fromCryptoError(cryptoError).asGrpcServiceException
    }
  }

  implicit class ApiErrorExt(private val apiError: ApiErrorBase) extends AnyVal {

    def asGrpcServiceException: GrpcServiceException =
      new GrpcServiceException(asGrpcStatus, asGrpcMetadata)

    def asGrpcMetadata: scaladsl.Metadata = {
      new MetadataBuilder()
        .addText(ErrorCodeMetadataKey, apiError.id.toString)
        .addText(ErrorMessageMetadataKey, apiError.message)
        .build()
    }

    def asGrpcStatus: Status = {
      apiError.code match {
        case StatusCodes.BadRequest          => Status.INVALID_ARGUMENT
        case StatusCodes.Unauthorized        => Status.UNAUTHENTICATED
        case StatusCodes.Forbidden           => Status.PERMISSION_DENIED
        case StatusCodes.NotFound            => Status.NOT_FOUND
        case StatusCodes.TooManyRequests     => Status.UNAVAILABLE
        case StatusCodes.InternalServerError => Status.INTERNAL
        case StatusCodes.NotImplemented      => Status.UNIMPLEMENTED
        case StatusCodes.BadGateway          => Status.UNAVAILABLE
        case StatusCodes.ServiceUnavailable  => Status.UNAVAILABLE
        case StatusCodes.GatewayTimeout      => Status.UNAVAILABLE
        case _                               => Status.UNKNOWN
      }
    }
  }
}
