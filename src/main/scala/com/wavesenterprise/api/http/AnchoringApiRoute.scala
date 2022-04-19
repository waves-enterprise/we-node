package com.wavesenterprise.api.http

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.service.AnchoringApiService
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.utils.Time

class AnchoringApiRoute(anchoringApiService: AnchoringApiService, val settings: ApiSettings, val time: Time, val nodeOwner: Address)
    extends ApiRoute {

  override val route: Route = {
    pathPrefix("anchoring") {
      withAuth() {
        config
      }
    }
  }

  /**
    * GET /anchoring/config
    **/
  def config: Route = get {
    complete(anchoringApiService.config)
  }
}
