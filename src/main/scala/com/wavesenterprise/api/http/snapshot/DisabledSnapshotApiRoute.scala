package com.wavesenterprise.api.http.snapshot

import akka.http.scaladsl.server.{Route, StandardRoute}
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.utils.Time

import scala.concurrent.Future

case class DisabledSnapshotApiRoute(settings: ApiSettings, time: Time, nodeOwner: Address) extends SnapshotApiRoute {
  private val disabledRoute: StandardRoute = complete(Future.successful(Left[ApiError, String](ApiError.SnapshotFeatureDisabled)))

  override protected def statusRoute: Route                             = disabledRoute
  override protected def genesisConfigRoute: Route                      = disabledRoute
  override protected def swapStateRoute(backupOldState: Boolean): Route = disabledRoute
}
