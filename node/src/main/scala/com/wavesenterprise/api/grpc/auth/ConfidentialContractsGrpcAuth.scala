package com.wavesenterprise.api.grpc.auth

import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.ConfidentialContractsApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.ConfidentialContractsUser
import monix.eval.Task

trait ConfidentialContractsGrpcAuth extends GrpcAuth {
  def withConfidentialContractsAuthTask[T](metadata: Metadata)(t: Task[T]): Task[T] =
    withAuthTask(
      metadata = metadata,
      protection = ConfidentialContractsApiKeyProtection,
      requiredRole = ConfidentialContractsUser
    )(t)

  def withConfidentialContractsAuth[T](metadata: Metadata)(f: => T): Either[GrpcServiceException, T] =
    withAuth(
      metadata = metadata,
      protection = ConfidentialContractsApiKeyProtection,
      requiredRole = ConfidentialContractsUser
    )(f)
}
