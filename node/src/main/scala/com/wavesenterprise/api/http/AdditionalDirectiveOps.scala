package com.wavesenterprise.api.http

import akka.http.scaladsl.server.Directive
import akka.http.scaladsl.server.Directives.pass

trait AdditionalDirectiveOps {
  protected def addedGuard: Directive[Unit] = pass

  protected def blockchainUpdaterGuard: Directive[Unit] = pass
}
