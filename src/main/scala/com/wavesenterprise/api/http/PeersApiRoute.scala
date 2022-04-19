package com.wavesenterprise.api.http

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.ApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.Administrator
import com.wavesenterprise.api.http.service.PeersApiService
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.utils.Time
import monix.execution.schedulers.SchedulerService

class PeersApiRoute(peersApiService: PeersApiService,
                    val settings: ApiSettings,
                    val time: Time,
                    val nodeOwner: Address,
                    val scheduler: SchedulerService)
    extends ApiRoute {

  override lazy val route =
    pathPrefix("peers") {
      allPeers ~ connectedPeers ~ suspendedPeers ~ allowedNodes ~ connect ~ hostname
    }

  private val userAuth  = withAuth()
  private val adminAuth = withAuth(ApiKeyProtection, Administrator)

  /**
    * GET /peers/all
    **/
  def allPeers: Route =
    (path("all") & get & userAuth) {
      withExecutionContext(scheduler) {
        complete(peersApiService.getAllPeers)
      }
    }

  /**
    * GET /peers/connected
    **/
  def connectedPeers: Route =
    (path("connected") & get & userAuth) {
      withExecutionContext(scheduler) {
        complete(peersApiService.connectedPeerList)
      }
    }

  /**
    * POST /peers/connect
    *
    * Connects to peer
    **/
  def connect: Route =
    (path("connect") & post & adminAuth) {
      withExecutionContext(scheduler) {
        json[ConnectReq](peersApiService.connectToPeer)
      }
    }

  /**
    * GET /peers/suspended
    **/
  def suspendedPeers: Route =
    (path("suspended") & get & userAuth) {
      withExecutionContext(scheduler) {
        complete(peersApiService.suspendedPeers)
      }
    }

  /**
    * GET /peers/allowedNodes
    *
    * List of registered participants (node addresses)
    **/
  def allowedNodes: Route =
    (path("allowedNodes") & get & adminAuth) {
      withExecutionContext(scheduler) {
        complete(peersApiService.getAllowedNodes)
      }
    }

  /**
    * GET /peers/hostname/{address}
    *
    * Get network info by address
    **/
  def hostname: Route = adminAuth {
    (path("hostname" / Segment) & get) { address =>
      withExecutionContext(scheduler) {
        complete(peersApiService.addressNetworkInfo(address))
      }
    }
  }
}
