package com.wavesenterprise.api.http.docker

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.AdditionalDirectiveOps
import com.wavesenterprise.api.http.auth.WithAuthFromContract
import com.wavesenterprise.api.http.service.ContractsApiService
import com.wavesenterprise.docker.ContractAuthTokenService
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.utils.Time
import monix.execution.schedulers.SchedulerService

/**
  * Contracts API for internal calls from contracts code
  */
class InternalContractsApiRoute(contractsApiService: ContractsApiService,
                                settings: ApiSettings,
                                time: Time,
                                contractAuthTokenServiceParam: ContractAuthTokenService,
                                externalNodeOwner: Address,
                                scheduler: SchedulerService)
    extends ContractsApiRoute(contractsApiService, settings, time, externalNodeOwner, scheduler)
    with AdditionalDirectiveOps
    with WithAuthFromContract {

  override val contractAuthTokenService: Option[ContractAuthTokenService] = Some(contractAuthTokenServiceParam)

  override lazy val route: Route =
    pathPrefix("internal" / "contracts") {
      addedGuard {
        withContractAuthClaim { claim =>
          val readingContext = ContractReadingContext.TransactionExecution(claim.txId)
          executedTransactionFor ~ contractKeys(readingContext) ~ contractKey(readingContext) ~ contracts ~ contractsState(readingContext)
        }
      }
    }
}
