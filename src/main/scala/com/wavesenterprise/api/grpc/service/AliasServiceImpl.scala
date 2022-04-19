package com.wavesenterprise.api.grpc.service

import akka.grpc.scaladsl.Metadata
import cats.implicits.catsSyntaxEither
import com.wavesenterprise.account.{Address, Alias}
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.api.grpc.utils.{ApiErrorExt, CryptoErrorExt}
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.AliasDoesNotExist
import com.wavesenterprise.protobuf.entity.{AddressRequest, AddressResponse}
import com.wavesenterprise.protobuf.service.alias.{AliasPublicServicePowerApi, AliasRequest, AliasesResponse}
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.Future

class AliasServiceImpl(
    val authSettings: AuthorizationSettings,
    val nodeOwner: Address,
    val time: Time,
    blockchain: Blockchain
)(implicit val s: Scheduler)
    extends AliasPublicServicePowerApi
    with GrpcAuth {
  override def addressByAlias(in: AliasRequest, metadata: Metadata): Future[AddressResponse] =
    withAuthTask(metadata) {
      Task.fromEither {
        (for {
          alias   <- Alias.buildWithCurrentChainId(in.alias).leftMap(ApiError.fromCryptoError)
          address <- blockchain.resolveAlias(alias).leftMap(_ => AliasDoesNotExist(alias))
        } yield AddressResponse(address.address)).leftMap(_.asGrpcServiceException)
      }
    }.runToFuture

  override def aliasesByAddress(in: AddressRequest, metadata: Metadata): Future[AliasesResponse] =
    withAuthTask(metadata) {
      Task.fromEither {
        Address
          .fromString(in.address)
          .map { address =>
            AliasesResponse(blockchain.aliasesOfAddress(address).map(_.stringRepr))
          }
          .leftMap(_.asGrpcServiceException)
      }
    }.runToFuture
}
