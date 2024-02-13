package com.wavesenterprise.api.grpc.service

import akka.grpc.scaladsl.Metadata
import cats.implicits._
import com.google.protobuf.empty.Empty
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.ValidInt._
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.api.grpc.utils.ApiErrorExt
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.DataKeyNotExists
import com.wavesenterprise.api.http.service.AddressApiService
import com.wavesenterprise.docker.grpc.ProtoObjectsMapper
import com.wavesenterprise.protobuf.entity.AddressesResponse
import com.wavesenterprise.protobuf.service.address.{AddressDataByKeyRequest, AddressDataRequest, AddressDataResponse, AddressPublicServicePowerApi}
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.Future

class AddressServiceImpl(
    val authSettings: AuthorizationSettings,
    val nodeOwner: Address,
    val time: Time,
    addressApiService: AddressApiService,
    blockchain: Blockchain
)(implicit val s: Scheduler)
    extends AddressPublicServicePowerApi
    with GrpcAuth {
  override def getAddresses(in: Empty, metadata: Metadata): Future[AddressesResponse] =
    withAuthTask(metadata) {
      Task(AddressesResponse(addressApiService.addressesFromWallet()))
    }.runToFuture

  override def getAddressData(in: AddressDataRequest, metadata: Metadata): Future[AddressDataResponse] =
    withAuthTask(metadata) {
      Task.fromEither {
        (List(in.offset.map(NonNegativeInt(_)), in.limit.map(PositiveInt(_))).flatten.toApiError >>
          addressApiService
            .accountData(in.address, in.offset, in.limit)
            .map { data =>
              AddressDataResponse(data.map(ProtoObjectsMapper.mapToProto))
            })
          .leftMap(_.asGrpcServiceException)
      }
    }.runToFuture

  override def getAddressDataByKey(in: AddressDataByKeyRequest, metadata: Metadata): Future[AddressDataResponse] =
    withAuthTask(metadata) {
      Task.fromEither {
        (for {
          address <- Address.fromString(in.address).leftMap(ApiError.fromCryptoError)
          data    <- blockchain.accountData(address, in.key.getOrElse("")).toRight(DataKeyNotExists(in.getKey))
        } yield AddressDataResponse(ProtoObjectsMapper.mapToProto(data) :: Nil)).leftMap(_.asGrpcServiceException)
      }
    }.runToFuture
}
