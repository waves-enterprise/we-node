package com.wavesenterprise.docker.grpc.service

import akka.grpc.scaladsl.Metadata
import cats.implicits._
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.service.PermissionApiService
import com.wavesenterprise.docker._
import com.wavesenterprise.docker.grpc.ProtoObjectsMapper
import com.wavesenterprise.protobuf.service.contract._
import com.wavesenterprise.utils.Time
import monix.execution.Scheduler

import scala.concurrent.Future

class PermissionServiceImpl(val time: Time,
                            val permissionApiService: PermissionApiService,
                            val contractAuthTokenService: ContractAuthTokenService,
                            val scheduler: Scheduler)
    extends PermissionServicePowerApi
    with WithServiceAuth {

  override def getPermissions(request: PermissionsRequest, metadata: Metadata): Future[PermissionsResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        val timestamp = request.timestamp.getOrElse(time.correctedTime())
        (for {
          response <- permissionApiService.forAddressAtTimestamp(request.address, timestamp)
          protoResponse = ProtoObjectsMapper.mapToProto(response)
        } yield protoResponse).leftMap(_.asGrpcServiceException)
      }
    ).runToFuture(scheduler)

  override def getPermissionsForAddresses(request: AddressesPermissionsRequest, metadata: Metadata): Future[AddressesPermissionsResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        (for {
          response <- permissionApiService.forAddressSeq(ProtoObjectsMapper.mapFromProto(request))
          protoResponse = ProtoObjectsMapper.mapToProto(response)
        } yield protoResponse).leftMap(_.asGrpcServiceException)
      }
    ).runToFuture(scheduler)
}
