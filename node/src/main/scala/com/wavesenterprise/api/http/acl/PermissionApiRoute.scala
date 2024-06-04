package com.wavesenterprise.api.http.acl

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.ValidInt._
import com.wavesenterprise.api.ValidLong._
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
      addressContractValidators ~ addressContractValidatorsHeight ~
        forAddressNow ~ forAddressAtTimestamp ~ forAddressSeq
    }
  }

  /**
    * GET /permissions/{addressOrAlias}
    *
    * Get all active permissions for address for current time
    **/
  def forAddressNow: Route = (get & path(Segment)) { addressOrAlias =>
    withExecutionContext(scheduler) {
      complete {
        permissionApiService.forAddressAtTimestamp(addressOrAlias, time.correctedTime())
      }
    }
  }

  /**
    * GET /permissions/{addressOrAlias}/at/{timestamp}
    *
    * Get all active permissions for address or alias at timestamp
    **/
  def forAddressAtTimestamp: Route = (get & path(Segment / "at" / Segment)) { (addressOrAlias, timestampStr) =>
    PositiveLong(timestampStr).processRoute { timestamp =>
      withExecutionContext(scheduler) {
        complete {
          permissionApiService.forAddressAtTimestamp(addressOrAlias, timestamp)
        }
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
        permissionApiService.addressContractValidator
      }
    }
  }

  /**
   * GET /permissions/contractValidators/{height}
   *
   * Get active roles contract-validators at height block
   * */
  def addressContractValidatorsHeight: Route = (path("contractValidators" / Segment) & get) { heightStr =>
    withExecutionContext(scheduler) {
      PositiveInt(heightStr).processRoute { height =>
        complete {
          permissionApiService.addressContractValidator(height)
        }
      }
    }
  }

  /**
    * POST /permissions/addresses
    *
    * Get active roles of given addresses or aliases at given timestamp
    **/
  def forAddressSeq: Route = (path("addresses") & post) {
    withExecutionContext(scheduler) {
      json[PermissionsForAddressesReq] { request =>
        permissionApiService.forAddressSeq(request)
      }
    }
  }

}
