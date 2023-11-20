package com.wavesenterprise.api.http.acl

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiRoute
import com.wavesenterprise.api.http.service.PermissionApiService
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utx.UtxPool
import monix.execution.schedulers.SchedulerService

class PermissionApiRoute(val settings: ApiSettings,
                         val utx: UtxPool,
                         val time: Time,
                         permissionApiService: PermissionApiService,
                         val nodeOwner: Address,
                         val scheduler: SchedulerService)
    extends ApiRoute {

  import PermissionApiService._

  override val route: Route = pathPrefix("permissions") {
    withAuth() {
      forAddressNow ~ forAddressAtTimestamp ~ forAddressSeq ~ addressContractValidators
    }
  }

  /**
    * GET
   * /permissions/{address}
    *
    * Get all active permissions for address for current time
    **/
  def forAddressNow: Route = (get & path(Segment)) { addressStr =>
    withExecutionContext(scheduler) {
      complete {
        permissionApiService.forAddressAtTimestamp(addressStr, time.correctedTime())
      }
    }
  }

  /**
    * GET /permissions/{address}/at/{timestamp}
    *
    * Get all active permissions for address at timestamp
    **/
  def forAddressAtTimestamp: Route = (get & path(Segment / "at" / LongNumber)) { (addressStr, timestamp) =>
    withExecutionContext(scheduler) {
      complete {
        permissionApiService.forAddressAtTimestamp(addressStr, timestamp)
      }
    }
  }

  /**
    * POST /permissions/addresses
    *
    * Get active roles of given addresses at given timestamp
    **/
  def forAddressSeq: Route = (path("addresses") & post) {
    withExecutionContext(scheduler) {
      json[PermissionsForAddressesReq] { request =>
        permissionApiService.forAddressSeq(request)
      }
    }
  }

  /**
   * GET /permissions/contractValidators
   *
   * Get active roles contract-validators at last block
   * */
  def addressContractValidators: Route = (path("contractValidators") & get) {
    withExecutionContext(scheduler) {
      complete {
        permissionApiService.contractValidate
      }
    }
  }

}
