package com.wavesenterprise.docker.grpc.service

import akka.grpc.scaladsl.Metadata
import cats.implicits._
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.docker._
import com.wavesenterprise.protobuf.service.contract._
import monix.execution.Scheduler
import scala.concurrent.Future

class PrivacyServiceImpl(
    val privacyApiService: PrivacyApiService,
    val contractAuthTokenService: ContractAuthTokenService,
    val scheduler: Scheduler
) extends PrivacyServicePowerApi
    with WithServiceAuth {

  override def getPolicyRecipients(request: PolicyRecipientsRequest, metadata: Metadata): Future[PolicyRecipientsResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        (for {
          recipients <- privacyApiService.policyRecipients(request.policyId)
          response = PolicyRecipientsResponse(recipients)
        } yield response).leftMap(_.asGrpcServiceException)
      }
    ).runToFuture(scheduler)

  override def getPolicyOwners(request: PolicyOwnersRequest, metadata: Metadata): Future[PolicyOwnersResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        (for {
          owners <- privacyApiService.policyOwners(request.policyId)
          response = PolicyOwnersResponse(owners)
        } yield response).leftMap(_.asGrpcServiceException)
      }
    ).runToFuture(scheduler)

  override def getPolicyHashes(request: PolicyHashesRequest, metadata: Metadata): Future[PolicyHashesResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        (for {
          hashes <- privacyApiService.policyHashes(request.policyId)
          response = PolicyHashesResponse(hashes.toSeq)
        } yield response).leftMap(_.asGrpcServiceException)
      }
    ).runToFuture(scheduler)

}
