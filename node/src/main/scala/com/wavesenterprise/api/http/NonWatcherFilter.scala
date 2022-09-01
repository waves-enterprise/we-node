package com.wavesenterprise.api.http

import akka.http.scaladsl.server.{Directive0, Directives}
import com.wavesenterprise.api.http.ApiError.IllegalWatcherActionError
import com.wavesenterprise.settings.NodeMode

trait NonWatcherFilter extends Directives {

  def nodeMode: NodeMode

  protected def nonWatcherFilter: Directive0 = nodeMode match {
    case NodeMode.Default => pass
    case NodeMode.Watcher => complete(IllegalWatcherActionError)
  }

}
