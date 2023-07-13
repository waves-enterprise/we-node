package com.wavesenterprise.docker.grpc.service

import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import cats.implicits._
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.docker.ContractExecutor.ContractTxClaimContent
import com.wavesenterprise.docker.grpc.GrpcContractExecutor.ConnectionClaimContent
import com.wavesenterprise.docker.{ClaimContent, ContractAuthTokenService, _}
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import play.api.libs.json.Reads

trait WithServiceAuth extends ScorexLogging {

  def contractAuthTokenService: ContractAuthTokenService

  private def checkAuthorized[T <: ClaimContent](metadata: Metadata)(implicit reads: Reads[T]): Either[GrpcServiceException, T] = {
    (for {
      authToken <- metadata
        .getText("Authorization")
        .toRight(ApiError.MissingAuthorizationMetadata)
      claimContent <- contractAuthTokenService
        .validate(authToken)
        .leftMap { throwable =>
          log.debug(s"Can't validate authorization token '$authToken': '$throwable'")
          ApiError.InvalidTokenError
        }
    } yield claimContent).leftMap(_.asGrpcServiceException)
  }

  protected def checkConnectionRequestAuthorized(metadata: Metadata): Either[GrpcServiceException, ConnectionClaimContent] = {
    checkAuthorized(metadata)(ConnectionClaimContent.reads)
  }

  protected def checkRequestAuthorized(metadata: Metadata): Either[GrpcServiceException, ContractTxClaimContent] = {
    checkAuthorized(metadata)(ContractTxClaimContent.format)
  }

  protected def withRequestAuth[T](metadata: Metadata, task: ContractTxClaimContent => Task[T]): Task[T] = {
    deferEither(checkRequestAuthorized(metadata)).flatMap(task)
  }

  protected def withRequestAuth[T](metadata: Metadata, task: Task[T]): Task[T] = {
    deferEither(checkRequestAuthorized(metadata)).flatMap(_ => task)
  }
}
