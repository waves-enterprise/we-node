package com.wavesenterprise.api.http.docker

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.ValidInt._
import com.wavesenterprise.api.http.ApiError.TooBigArrayAllocation
import com.wavesenterprise.api.http.service.ContractsApiService
import com.wavesenterprise.api.http.{ApiError, ApiRoute}
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.utils.Time
import monix.execution.schedulers.SchedulerService
import play.api.libs.json.{Json, OFormat}

class ContractsApiRoute(val contractsApiService: ContractsApiService,
                        val settings: ApiSettings,
                        val time: Time,
                        val nodeOwner: Address,
                        scheduler: SchedulerService)
    extends ApiRoute {

  import ContractsApiRoute._

  override lazy val route: Route =
    pathPrefix("contracts") {
      withAuth() {
        executedTransactionFor ~ executionStatus ~ contractInfo ~ contractKeys() ~ contractKey() ~ contractKeysFiltered() ~ contracts ~ contractsState()
      }
    }

  /**
    * GET /contracts
    **/
  def contracts: Route = (get & pathEnd) {
    withExecutionContext(scheduler) {
      complete(contractsApiService.contracts())
    }
  }

  /**
    * GET /contracts/info/{contractId}
    **/
  def contractInfo: Route = (get & path("info" / Segment)) { contractId =>
    withExecutionContext(scheduler) {
      complete(contractsApiService.contractInfo(contractId))
    }
  }

  /**
    * POST /contracts
    *
    * Returns state for few contracts
    **/
  def contractsState(readingContext: ContractReadingContext = ContractReadingContext.Default): Route = (post & pathEnd) {
    withExecutionContext(scheduler) {
      json[ContractsIdMessage] { message =>
        ContractsIdMessage.validate(message).flatMap { _ =>
          contractsApiService.contractKeys(message.contracts, readingContext)
        }
      }
    }
  }

  /**
    * GET /contracts/{contractId}
    *
    * Get keys values by contract id
    **/
  def contractKeys(readingContext: ContractReadingContext = ContractReadingContext.Default): Route = (get & path(Segment)) { contractId =>
    withExecutionContext(scheduler) {
      parameters('offset.as[String].?, 'limit.as[String].?, 'matches.as[String].?) { (offset, limit, matches) =>
        List(offset, limit)
          .flatMap(_.map(PositiveInt(_)))
          .processRoute { _ =>
            complete(contractsApiService.contractKeys(contractId, offset.map(_.toInt), limit.map(_.toInt), matches, readingContext))
          }
      }
    }
  }

  /**
    * GET /contracts/{contractId}/{key}
    *
    * Get key value by contract id
    **/
  def contractKey(readingContext: ContractReadingContext = ContractReadingContext.Default): Route = (get & path(Segment / Segment)) {
    case (contractId, key) =>
      withExecutionContext(scheduler) {
        complete(contractsApiService.contractKey(contractId, key, readingContext))
      }
  }

  /**
    * POST /contracts/{contractId}
    *
    * Get keys values by contract id
    **/
  def contractKeysFiltered(readingContext: ContractReadingContext = ContractReadingContext.Default): Route = (post & path(Segment)) { contractId =>
    withExecutionContext(scheduler) {
      json[ContractKeysFilter] { keysFilter =>
        ContractKeysFilter.validate(keysFilter).flatMap { _ =>
          contractsApiService.contractKeys(contractId, keysFilter.keys, readingContext)
        }
      }
    }
  }

  /**
    * GET /contracts/executed-tx-for/{id}
    *
    * Returns executed transaction, if present, for id of executable transaction
    **/
  def executedTransactionFor: Route = (get & pathPrefix("executed-tx-for")) {
    withExecutionContext(scheduler) {
      path(Segment) { id =>
        complete(contractsApiService.executedTransactionFor(id))
      }
    }
  }

  /**
    * GET /contracts/status/{id}
    *
    * Returns array of execution statuses for executable transaction
    **/
  def executionStatus: Route = (get & pathPrefix("status")) {
    withExecutionContext(scheduler) {
      path(Segment) { id =>
        complete(contractsApiService.executionStatuses(id))
      }
    }
  }
}

object ContractsApiRoute {

  val MaxContractsPerRequest = 100
  val MaxKeysPerRequest      = 100

  case class ContractsIdMessage(contracts: Set[String])

  object ContractsIdMessage {
    def validate(message: ContractsIdMessage): Either[ApiError, ContractsIdMessage] = {
      Either.cond(message.contracts.size <= MaxContractsPerRequest, message, TooBigArrayAllocation)
    }
  }

  implicit val contractsIdArrayFormat: OFormat[ContractsIdMessage] = Json.format

  case class ContractKeysFilter(keys: Set[String])

  object ContractKeysFilter {
    def validate(keysFilter: ContractKeysFilter): Either[ApiError, ContractKeysFilter] = {
      Either.cond(keysFilter.keys.size <= MaxKeysPerRequest, keysFilter, TooBigArrayAllocation)
    }
  }

  implicit val contractKeysFilterFormat: OFormat[ContractKeysFilter] = Json.format
}
