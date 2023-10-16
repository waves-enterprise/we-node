package com.wavesenterprise.api.http.docker.confidential

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.ValidInt._
import com.wavesenterprise.api.http.ApiRoute
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.ConfidentialContractsApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.ConfidentialContractsUser
import com.wavesenterprise.api.http.service.ContractsApiService
import com.wavesenterprise.api.http.service.confidentialcontract.{ConfidentialContractCallRequest, ConfidentialContractsApiService}
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.utils.Time
import monix.execution.schedulers.SchedulerService

class ConfidentialContractsApiRoute(
    val contractsApiService: ContractsApiService,
    val confidentialContractsApiService: ConfidentialContractsApiService,
    val settings: ApiSettings,
    val time: Time,
    val nodeOwner: Address,
    val scheduler: SchedulerService
) extends ApiRoute {

  override lazy val route: Route =
    pathPrefix("confidential-contracts") {
      withAuth(ConfidentialContractsApiKeyProtection, ConfidentialContractsUser) {
        confidentialContractKeys ~ confidentialExecutedTxByExecutableTxId ~ confidentialContractsCall
      }
    }

  /**
    * POST /confidential-contracts/call
    */
  protected def confidentialContractsCall: Route =
    (post & path("call")) {
      withExecutionContext(scheduler) {
        parameters(
          "broadcast".as[Boolean] ? true,
          "commitmentVerification".as[Boolean] ? false
        ) { (broadcast, commitmentVerification) =>
          json[ConfidentialContractCallRequest] { request =>
            confidentialContractsApiService.call(request, broadcast, commitmentVerification).runToFuture(scheduler)
          }
        }
      }
    }

  /**
    * GET /confidential-contracts/{contractId}
    *
    * Get confidential keys values by contract id
    * */
  protected def confidentialContractKeys: Route = (get & path(Segment)) { contractId =>
    withExecutionContext(scheduler) {
      parameters('offset.as[String].?, 'limit.as[String].?, 'matches.as[String].?) { (offset, limit, matches) =>
        List(offset, limit)
          .flatMap(_.map(PositiveInt(_)))
          .processRoute { _ =>
            complete(confidentialContractsApiService.contractKeys(contractId, offset.map(_.toInt), limit.map(_.toInt), matches))
          }
      }
    }
  }

  /**
    * GET /confidential-contracts/tx/{executable-tx-id}
    *
    * Get get confidential executed tx, input and output by executable transaction id
    * */
  protected def confidentialExecutedTxByExecutableTxId: Route = (get & pathPrefix("tx")) {
    withExecutionContext(scheduler) {
      path(Segment) { executableTxId =>
        complete(confidentialContractsApiService.confidentialTxByExecutableTxId(executableTxId))
      }
    }
  }
}
