package com.wavesenterprise.api.grpc.auth

import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import com.wavesenterprise.api.grpc.utils.EitherApiErrorExt
import com.wavesenterprise.api.http.ApiError.PrivacyIsSwitchedOff
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.PrivacyApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.PrivacyUser
import monix.eval.Task

trait PrivacyGrpcAuth extends GrpcAuth {
  def privacyEnabled: Boolean

  def withPrivacyAuthTask[T](metadata: Metadata)(t: Task[T]): Task[T] = {
    Either.cond(privacyEnabled, (), PrivacyIsSwitchedOff).asTask >>
      withAuthTask(metadata = metadata, protection = PrivacyApiKeyProtection, requiredRole = PrivacyUser)(t)
  }

  def withPrivacyAuth[T](metadata: Metadata)(f: => T): Either[GrpcServiceException, T] = {
    withAuth(metadata = metadata, protection = PrivacyApiKeyProtection, requiredRole = PrivacyUser)(f)
  }
}
