package com.wavesenterprise.api.http.snapshot

import akka.http.scaladsl.server.{Directive0, Route}
import com.wavesenterprise.api.http.ApiRoute
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.ApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.Administrator

trait SnapshotApiRoute extends ApiRoute {
  override lazy val route: Route =
    pathPrefix("snapshot") {
      status ~ genesisConfig ~ swapState
    }

  private val userAuth: Directive0            = withAuth()
  protected def adminAuthOrApiKey: Directive0 = withAuth(ApiKeyProtection, Administrator)

  final def status: Route        = (get & path("status") & userAuth)(statusRoute)
  final def genesisConfig: Route = (get & path("genesisConfig") & userAuth)(genesisConfigRoute)
  final def swapState: Route     = (post & path("swapState") & adminAuthOrApiKey & parameter('backupOldState.as[Boolean] ? false))(swapStateRoute)

  protected def statusRoute: Route
  protected def genesisConfigRoute: Route
  protected def swapStateRoute(backupOldState: Boolean): Route
}
