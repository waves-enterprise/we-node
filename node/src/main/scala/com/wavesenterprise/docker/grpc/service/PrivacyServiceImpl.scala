package com.wavesenterprise.docker.grpc.service

import akka.grpc.scaladsl.Metadata
import cats.implicits._
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.docker._
import com.wavesenterprise.docker.grpc.ProtoObjectsMapper
import com.wavesenterprise.protobuf.service.contract._
import monix.eval.Task
import monix.execution.Scheduler
import com.wavesenterprise.utils.Base64
import scala.concurrent.{ExecutionContext, Future}

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

  override def getPolicyItemData(request: PolicyItemDataRequest, metadata: Metadata): Future[PolicyItemDataResponse] =
    withRequestAuth(
      metadata,
      Task.deferFuture {
        privacyApiService
          .policyItemData(request.policyId, request.itemHash)
          .flatMap {
            case Right(value) =>
              implicit val ec: ExecutionContext = scheduler
              Future {
                val encoded = Base64.encode(value.arr)
                PolicyItemDataResponse(encoded)
              }
            case Left(error) =>
              Future.failed(error.asGrpcServiceException)
          }(scheduler)
      }
    ).runToFuture(scheduler)

  override def getPolicyItemInfo(request: PolicyItemInfoRequest, metadata: Metadata): Future[PolicyItemInfoResponse] =
    withRequestAuth(
      metadata,
      Task.deferFuture {
        privacyApiService
          .policyItemInfo(request.policyId, request.itemHash)
          .flatMap {
            case Right(value) => Future.successful(ProtoObjectsMapper.mapToProto(value))
            case Left(error)  => Future.failed(error.asGrpcServiceException)
          }(scheduler)
      }
    ).runToFuture(scheduler)
}
