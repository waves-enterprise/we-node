package com.wavesenterprise.api.grpc.service

import akka.NotUsed
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import akka.stream.scaladsl.Source
import cats.syntax.either.catsSyntaxEither
import com.google.protobuf.empty.Empty
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.api.http.service.ContractsApiService
import com.wavesenterprise.protobuf.service.util.{ContractExecutionRequest, ContractExecutionResponse, ContractStatusServicePowerApi}
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.settings.api.ContractStatusServiceSettings
import com.wavesenterprise.utils.Time
import io.grpc.Status
import monix.eval.Task
import monix.execution.schedulers.SchedulerService

class ContractStatusServiceImpl(val settings: ContractStatusServiceSettings,
                                val contractsApiService: ContractsApiService,
                                val authSettings: AuthorizationSettings,
                                val nodeOwner: Address,
                                val time: Time,
                                val scheduler: SchedulerService)
    extends ContractStatusServicePowerApi
    with GrpcAuth
    with ConnectionsLimiter {

  override def contractExecutionStatuses(request: ContractExecutionRequest, metadata: Metadata): Source[ContractExecutionResponse, NotUsed] =
    withAuth(metadata) {
      contractsApiService
        .executionStatuses(request.txId)
        .map(_.map(_.toProto))
        .leftMap(err => new GrpcServiceException(Status.UNKNOWN.withDescription(err.message)))
    }.joinRight match {
      case Left(err)       => Source.failed(err)
      case Right(statuses) => Source.apply(statuses.toList)
    }

  override def contractsExecutionEvents(in: Empty, metadata: Metadata): Source[ContractExecutionResponse, NotUsed] = {
    withConnectionsLimiter(metadata, in.toProtoString) { connectionId =>
      withAuth(metadata) {
        contractsApiService.lastMessage
          .doOnNext { message =>
            Task.eval(log.trace(s"[$connectionId] $message"))
          }
          .map(_.toProto)
          .executeOn(scheduler)
      }
    }
  }

  override val maxConnections: Int = settings.maxConnections
}
