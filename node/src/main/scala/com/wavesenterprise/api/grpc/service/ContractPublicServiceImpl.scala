package com.wavesenterprise.api.grpc.service

import akka.grpc.scaladsl.Metadata
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.api.http.service.ContractsApiService
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.utils.Time
import monix.execution.schedulers.SchedulerService
import cats.implicits._
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.protobuf.service.contract.{
  ContractBalanceResponse,
  ContractBalancesResponse,
  ContractBalancesByIdRequest,
  ContractPublicServicePowerApi
}
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import monix.eval.Task

import scala.concurrent.Future

class ContractPublicServiceImpl(val contractsApiService: ContractsApiService,
                                val authSettings: AuthorizationSettings,
                                val nodeOwner: Address,
                                val time: Time,
                                val scheduler: SchedulerService)
    extends ContractPublicServicePowerApi
    with GrpcAuth {

  override def getContractBalances(in: ContractBalancesByIdRequest, metadata: Metadata): Future[ContractBalancesResponse] =
    withAuthTask(metadata) {
      Task.fromEither {
        val balancesEither = in.assetsIds
          .map { pbAssetId =>
            val readingContext = ContractReadingContext.Default

            val assetIdOpt = Option(pbAssetId).filter(_.nonEmpty)
            contractsApiService.contractAssetBalance(in.contractId, assetIdOpt, readingContext).map { contractAssetBalance =>
              ContractBalanceResponse(assetIdOpt, contractAssetBalance.amount, contractAssetBalance.decimals)
            }
          }
          .toList
          .sequence

        balancesEither
          .map(ContractBalancesResponse(_))
          .leftMap(_.asGrpcServiceException)
      }
    }.runToFuture(scheduler)

}
